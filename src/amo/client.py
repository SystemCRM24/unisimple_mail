import logging
from typing import Self, Optional, List, Dict, Any

from aiohttp import ClientSession, ClientResponseError
from aiolimiter import AsyncLimiter

from src.settings import settings

logger = logging.getLogger(__name__)


class AmoClient:
    """
    Клиент для взаимодействия с API amoCRM.

    """
    _session: ClientSession
    _API_VERSION = "v4"

    pipelines_ids: Dict[str, int]
    statuses_ids: Dict[int, Dict[str, int]]
    users_ids: Dict[str, int]
    custom_fields_lead_ids: Dict[str, int]
    custom_fields_company_ids: Dict[str, int]
    task_types_ids: Dict[str, int]

    def __init__(self):
        self._headers = {
            'Authorization': f'Bearer {settings.current_amo_long_term_token}',
            'Content-Type': 'application/json'
        }
        self._base_url = f"https://{settings.current_amo_subdomain}.amocrm.ru/api/{self._API_VERSION}"
        rate = 1.0 / settings.request_delay if settings.request_delay > 0 else 2.0
        self._rate_limit = AsyncLimiter(max_rate=rate, time_period=1)
        self._initialized_ids = False

        self.pipelines_ids = {}
        self.statuses_ids = {}
        self.users_ids = {}
        self.custom_fields_lead_ids = {}
        self.custom_fields_company_ids = {}
        self.task_types_ids = {}


    async def __aenter__(self) -> Self:
        """
        Входит в асинхронный контекст.
        Создает сессию aiohttp и выполняет инициализацию справочников ID.
        Returns:
            Экземпляр клиента AmoClient.
        """
        self._session = ClientSession(headers=self._headers, trust_env=True)
        await self._ensure_ids_initialized()
        return self


    async def __aexit__(self, *args) -> None:
        """
        Выходит из асинхронного контекста. 
        """
        if self._session and not self._session.closed:
            await self._session.close()


    async def _request(self, method: str, url: str, json_data: Optional[Dict[str, Any]] = None,
                       params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Выполняет асинхронный HTTP-запрос к API.
        Args:
            method: HTTP-метод запроса.
            url: Часть URL-пути после базового URL.
            json_data: Словарь с данными для отправки в теле запроса в формате JSON.
            params: Словарь с параметрами для добавления к URL в виде query string.
        Returns:
            В случае успешного выполнения запроса (статус 2xx, кроме 204)
            возвращает словарь с JSON-ответом от сервера.
            В случае ошибки (статус 4xx или 5xx) или другого исключения,
            логирует ошибку и вызывает исключение повторно.
        """
        full_url = f"{self._base_url}{url}"
        async with self._rate_limit:
            try:
                kwargs = {}
                if json_data:
                    kwargs['json'] = json_data
                if params:
                    kwargs['params'] = params
                logger.debug(f"AmoAPI Request: {method} {full_url} | Params: {params} | JSON: {json_data is not None}")
                async with self._session.request(method, full_url, **kwargs) as response:
                    logger.debug(f"AmoAPI Response Status: {response.status} for {full_url}")
                    if 200 <= response.status < 300:
                        if response.status == 204:
                            return None
                        return await response.json()
                    else:
                        response_text = await response.text()
                        logger.error(
                            f"API request error: {method} {full_url}, Status: {response.status}, Response: {response_text[:500]}"
                        )
                        response.raise_for_status()
            except ClientResponseError as e:
                logger.error(f"ClientResponseError for {method} {full_url}: {e.status} {e.message}")
                raise 
            except Exception as e:
                logger.error(f"Unexpected error during request to {full_url}: {e}", exc_info=True)
                raise
        return None


    async def _get_all_pages(self, endpoint: str, entity_key_in_embedded: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Собирает данные со всех страниц с учетом пагинации.
        Args:
            endpoint: Эндпоинт API (например, '/leads').
            entity_key_in_embedded: Ключ в словаре '_embedded' ответа,
                                    содержащий список сущностей (например, 'leads').
            params: Дополнительные параметры запроса.
        Returns:
            Список словарей, представляющих все сущности, полученные со всех страниц.
        """
        all_data: List[Dict[str, Any]] = []
        page = 1
        while True:
            current_params = params.copy() if params else {}
            current_params['page'] = page
            current_params['limit'] = 250
            try:
                response = await self._request('GET', endpoint, params=current_params)
            except Exception:
                logger.error(f"API error or unexpected error fetching page {page} for {endpoint}. Stopping pagination.", exc_info=True)
                break

            if not response or '_embedded' not in response or entity_key_in_embedded not in response['_embedded']:
                if page == 1 and response and '_embedded' in response and not response['_embedded'].get(entity_key_in_embedded):
                    logger.debug(f"No entities '{entity_key_in_embedded}' found on first page for {endpoint}.")
                break

            entities = response['_embedded'][entity_key_in_embedded]
            if not isinstance(entities, list):
                if entities:
                    all_data.append(entities)
                else:
                    break
            else:
                if not entities:
                    break
                all_data.extend(entities)

            if response.get('_links', {}).get('next'):
                page += 1
            else:
                break
        logger.debug(f"Fetched {len(all_data)} items for '{entity_key_in_embedded}' from {endpoint}")
        return all_data


    async def _ensure_ids_initialized(self):
        """
        Проверяет, были ли инициализированы ID справочников, и если нет,
        загружает их из API и кэширует.

        """
        if self._initialized_ids:
            return
        logger.info("Инициализация справочников ID из amoCRM...")
        try:
            pipelines_data = await self._get_all_pages('/leads/pipelines', 'pipelines')
            for p in pipelines_data:
                self.pipelines_ids[p['name']] = p['id']
                self.statuses_ids[p['id']] = {s['name']: s['id'] for s in p.get('_embedded', {}).get('statuses', [])}

            users_data = await self._get_all_pages('/users', 'users')
            for u in users_data:
                self.users_ids[u['name']] = u['id']

            lead_fields_data = await self._get_all_pages('/leads/custom_fields', 'custom_fields')
            for cf in lead_fields_data:
                self.custom_fields_lead_ids[cf['name']] = cf['id']

            company_fields_data = await self._get_all_pages('/companies/custom_fields', 'custom_fields')
            for cf in company_fields_data:
                self.custom_fields_company_ids[cf['name']] = cf['id']
                
            self.custom_fields_company_ids[settings.CUSTOM_FIELD_NAME_COMPANY_PHONE] = next(
                (cf['id'] for cf in company_fields_data if cf['name'] == settings.CUSTOM_FIELD_NAME_COMPANY_PHONE), None)
            self.custom_fields_company_ids[settings.CUSTOM_FIELD_NAME_COMPANY_EMAIL] = next(
                (cf['id'] for cf in company_fields_data if cf['name'] == settings.CUSTOM_FIELD_NAME_COMPANY_EMAIL), None)

            self.task_types_ids = {}
            logger.info("Загрузка типов задач пропущена (эндпоинт /api/v4/tasks/types недоступен).")

            self._initialized_ids = True
            logger.info("Инициализация ID из amoCRM (кроме типов задач) успешно завершена.")
        except Exception as e:
            logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА при инициализации ID из amoCRM: {e}", exc_info=True)
            raise RuntimeError(f"Failed to initialize IDs from AmoCRM: {e}")


    async def get_pipeline_id(self, pipeline_name: str) -> Optional[int]:
        """
        Возвращает ID воронки по ее имени.

        Args:
            pipeline_name: Имя воронки.
        Returns:
            Целочисленный ID воронки или None, если воронка не найдена.
        """
        return self.pipelines_ids.get(pipeline_name)


    async def get_status_id(self, pipeline_id: int, status_name: str) -> Optional[int]:
        """
        Возвращает ID статуса сделки по ID воронки и имени статуса.

        Args:
            pipeline_id: ID воронки.
            status_name: Имя статуса.
        Returns:
            Целочисленный ID статуса или None, если воронка или статус не найдены.
        """
        return self.statuses_ids.get(pipeline_id, {}).get(status_name)


    async def get_user_id(self, user_name: str) -> Optional[int]:
        """
        Возвращает ID пользователя по его имени.

        Args:
            user_name: Имя пользователя.
        Returns:
            Целочисленный ID пользователя или None, если пользователь не найден.
        """
        return self.users_ids.get(user_name)


    async def get_custom_field_id_lead(self, field_name: str) -> Optional[int]:
        """
        Возвращает ID пользовательского поля для сделок по его имени.

        Args:
            field_name: Имя пользовательского поля.
        Returns:
            Целочисленный ID поля или None, если поле не найдено.
        """
        return self.custom_fields_lead_ids.get(field_name)


    async def get_custom_field_id_company(self, field_name: str) -> Optional[int]:
        """
        Возвращает ID пользовательского поля для компаний по его имени.

        Args:
            field_name: Имя пользовательского поля.
        Returns:
            Целочисленный ID поля или None, если поле не найдено.
        """
        return self.custom_fields_company_ids.get(field_name)


    async def get_task_type_id(self, task_type_name: str) -> Optional[int]:
        """
        Возвращает ID типа задачи по его имени.

        Args:
            task_type_name: Имя типа задачи.
        Returns:
            Целочисленный ID типа задачи или None, если тип задачи не найден.
        """
        return self.task_types_ids.get(task_type_name)


    async def search_companies_by_inn(self, inn: str) -> List[Dict[str, Any]]:
        """
        Ищет компании по ИНН (пользовательское поле).
        Возвращает список найденных компаний.
        Args:
            inn: Значение ИНН для поиска.
        Returns:
            Список словарей, представляющих найденные компании.
        """
        inn_field_id = self.custom_fields_company_ids.get(settings.CUSTOM_FIELD_NAME_INN_LEAD)
        if not inn_field_id:
            logger.warning(f"Пользовательское поле '{settings.CUSTOM_FIELD_NAME_INN_LEAD}' (ИНН) не найдено для компаний. Поиск по ИНН невозможен.")
            return []

        params = {
            'query': inn,
            'with': 'custom_fields'
        }
        companies = await self._get_all_pages('/companies', 'companies', params=params)
        found_companies = []
        for company in companies:
            if 'custom_fields_values' in company:
                for cf_value in company['custom_fields_values']:
                    if cf_value['field_id'] == inn_field_id:
                        for value in cf_value['values']:
                            if value['value'] == inn:
                                found_companies.append(company)
                                break
                        break
        return found_companies


    async def create_company(
        self, 
        name: str, 
        responsible_user_id: Optional[int] = None,
        inn: Optional[str] = None, phone_numbers: Optional[List[str]] = None,
        emails: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Создает новую компанию в amoCRM.
        Args:
            name: Название компании.
            responsible_user_id: ID ответственного пользователя.
            inn: ИНН компании.
            phone_numbers: Список телефонных номеров компании.
            emails: Список email-адресов компании.
        Returns:
            Словарь, представляющий созданную компанию, или None в случае ошибки.
        """
        payload_item: Dict[str, Any] = {"name": name}

        if responsible_user_id:
            payload_item["responsible_user_id"] = responsible_user_id

        custom_fields_values: List[Dict[str, Any]] = []

        if inn:
            inn_field_id = self.custom_fields_company_ids.get(settings.CUSTOM_FIELD_NAME_INN_COMPANY)
            if inn_field_id:
                custom_fields_values.append({
                    "field_id": inn_field_id,
                    "values": [{"value": inn}]
                })
            else:
                logger.warning(f"Пользовательское поле для ИНН компании ('{settings.CUSTOM_FIELD_NAME_INN_COMPANY}') не найдено. ИНН не будет добавлено.")

        if phone_numbers:
            phone_field_id = self.custom_fields_company_ids.get("Телефон")
            if phone_field_id:
                phone_values = [{"value": num, "enum_code": "WORK"} for num in phone_numbers]
                custom_fields_values.append({
                    "field_id": phone_field_id,
                    "values": phone_values
                })
            else:
                logger.warning("Системное поле 'Телефон' для компаний не найдено. Телефоны не будут добавлены.")

        if emails:
            email_field_id = self.custom_fields_company_ids.get("Email")
            if email_field_id:
                email_values = [{"value": email, "enum_code": "WORK"} for email in emails]
                custom_fields_values.append({
                    "field_id": email_field_id,
                    "values": email_values
                })
            else:
                logger.warning("Системное поле 'Email' для компаний не найдено. Email-ы не будут добавлены.")

        if custom_fields_values:
            payload_item["custom_fields_values"] = custom_fields_values

        payload = [payload_item]
        try:
            response = await self._request('POST', "/companies", json_data=payload)
            if response and '_embedded' in response and 'companies' in response['_embedded']:
                created_company = response['_embedded']['companies'][0]
                logger.info(f"Компания '{created_company.get('name')}' (ID: {created_company.get('id')}) успешно создана.")
                return created_company
            else:
                logger.error(f"Неожиданный ответ при создании компании: {response}")
                return None
        except ClientResponseError as e:
            logger.error(f"Ошибка при создании компании '{name}': {e.status}, message='{e.message}', url='{e.url}'")
            return None
        except Exception as e:
            logger.error(f"Неизвестная ошибка при создании компании '{name}': {e}", exc_info=True)
            return None


    async def search_leads_by_name(self, pipeline_id: int, purchase_number:str) -> List[Dict[str, Any]]:
        """
        Ищет сделки в конкретной воронке по номеру закупки.
        Args:
            pipeline_id: ID воронки.
            purchase_number: Номер закупки для поиска.
        Returns:
            Список словарей, представляющих найденные сделки.
        """
        purchase_number_field_id = self.custom_fields_lead_ids.get(settings.CUSTOM_FIELD_NAME_PURCHASE_NUMBER)
        if not purchase_number_field_id:
            logger.warning(f"Пользовательское поле '{settings.CUSTOM_FIELD_NAME_PURCHASE_NUMBER}' не найдено для сделок. Поиск по номеру закупки невозможен.")
            return []

        params = {
            'query': purchase_number,
            'filter[pipelines][0][id]': pipeline_id,
        }

        leads = await self._get_all_pages('/leads', 'leads', params=params)
        
        filtered_leads = []
        for lead in leads:
            if 'custom_fields_values' in lead:
                for cf_value in lead['custom_fields_values']:
                    if cf_value['field_id'] == purchase_number_field_id:
                        for value_obj in cf_value.get('values', []):
                            if value_obj.get('value') == purchase_number:
                                filtered_leads.append(lead)
                                break
                        if filtered_leads and filtered_leads[-1]['id'] == lead['id']:
                            break
        return filtered_leads
    

    async def search_leads_by_inn(self, pipeline_id: int, inn: str) -> List[Dict[str, Any]]:
        """Ищет сделки в конкретной воронке по ИНН клиента."""
        inn_field_id = self.custom_fields_lead_ids.get(settings.CUSTOM_FIELD_NAME_INN_LEAD)
        if not inn_field_id:
            logger.warning(f"Пользовательское поле '{settings.CUSTOM_FIELD_NAME_INN_LEAD}' не найдено для сделок. Поиск по номеру закупки невозможен.")
            return []
        params = {'query': inn, 'filter[pipelines][0][id]': pipeline_id}
        leads = await self._get_all_pages('/leads', 'leads', params=params)
        filtered_leads = []
        for lead in leads:
            if 'custom_fields_values' in lead:
                for cf_value in lead['custom_fields_values']:
                    if cf_value['field_id'] == inn_field_id:
                        for value_obj in cf_value.get('values', []):
                            if value_obj.get('value') == inn:
                                filtered_leads.append(lead)
                                break
                        if filtered_leads and filtered_leads[-1]['id'] == lead['id']:
                            break
        return filtered_leads


    async def create_lead(
        self, name: str, 
        price: float, 
        pipeline_id: int, 
        status_id: int, 
        responsible_user_id: Optional[int] = None, 
        company_id: Optional[int] = None, 
        custom_fields: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Создает новую сделку в amoCRM.
        Args:
            name: Название сделки.
            price: Бюджет сделки.
            pipeline_id: ID воронки.
            status_id: ID статуса в воронке.
            responsible_user_id: ID ответственного пользователя.
            company_id: ID связанной компании.
            custom_fields: Список пользовательских полей для заполнения.
        Returns:
            Словарь, представляющий созданную сделку, или None в случае ошибки.
        """
        payload_item: Dict[str, Any] = {
            "name": name,
            "price": int(price),
            "pipeline_id": pipeline_id,
            "status_id": status_id,
        }
        if responsible_user_id:
            payload_item["responsible_user_id"] = responsible_user_id
        if company_id:
            payload_item["_embedded"] = {"companies": [{"id": company_id}]}

        if custom_fields:
            formatted_custom_fields = []
            for field in custom_fields:
                field_id = self.custom_fields_lead_ids.get(field["field_name"])
                if field_id:
                    formatted_custom_fields.append({"field_id": field_id, "values": [{"value": v} for v in field["values"]]})
                else:
                    logger.warning(f"Пользовательское поле '{field['field_name']}' не найдено. Пропускаем.")
            if formatted_custom_fields:
                payload_item["custom_fields_values"] = formatted_custom_fields

        payload = [payload_item]
        try:
            response = await self._request('POST', '/leads', json_data=payload)
            if response and '_embedded' in response and 'leads' in response['_embedded'] and response['_embedded']['leads']:
                created_lead = response['_embedded']['leads'][0]
                logger.info(f"Создана сделка '{name}' (ID: {created_lead.get('id')}).")
                return created_lead
            return None
        except Exception as e:
            logger.error(f"Ошибка при создании сделки '{name}': {e}", exc_info=True)
            return None


    async def update_lead(
            self, 
            lead_id: int, 
            name: Optional[str] = None, 
            price: Optional[float] = None, 
            status_id: Optional[int] = None, 
            responsible_user_id: Optional[int] = None, 
            custom_fields: Optional[List[Dict[str, Any]]] = None
        ) -> Optional[Dict[str, Any]]:
        """
        Обновляет существующую сделку в amoCRM.
        Args:
            lead_id: ID сделки, которую нужно обновить.
            name: Новое название сделки (опционально).
            price: Новый бюджет сделки (опционально).
            status_id: Новый ID статуса (опционально).
            responsible_user_id: Новый ID ответственного (опционально).
            custom_fields: Список пользовательских полей для обновления.
        Returns:
            Словарь, представляющий обновленную сделку, или None в случае ошибки.
        """
        payload_item: Dict[str, Any] = {"id": lead_id}
        if name:
            payload_item["name"] = name
        if price is not None:
            payload_item["price"] = int(price)
        if status_id:
            payload_item["status_id"] = status_id
        if responsible_user_id:
            payload_item["responsible_user_id"] = responsible_user_id

        if custom_fields:
            formatted_custom_fields = []
            for field in custom_fields:
                field_id = self.custom_fields_lead_ids.get(field["field_name"])
                if field_id:
                    formatted_custom_fields.append({"field_id": field_id, "values": [{"value": v} for v in field["values"]]})
                else:
                    logger.warning(f"Пользовательское поле '{field['field_name']}' не найдено. Пропускаем при обновлении.")
            if formatted_custom_fields:
                payload_item["custom_fields_values"] = formatted_custom_fields

        payload = [payload_item]
        try:
            response = await self._request('PATCH', '/leads', json_data=payload)
            if response and '_embedded' in response and 'leads' in response['_embedded'] and response['_embedded']['leads']:
                updated_lead = response['_embedded']['leads'][0]
                logger.info(f"Сделка ID {lead_id} успешно обновлена.")
                return updated_lead
            return None
        except Exception as e:
            logger.error(f"Ошибка при обновлении сделки ID {lead_id}: {e}", exc_info=True)
            return None


    async def add_note_to_lead(self, lead_id: int, text: str) -> Optional[Dict[str, Any]]:
        """
        Добавляет примечание к сделке.
        Args:
            lead_id: ID сделки, к которой добавляется примечание.
            text: Текст примечания.
        Returns:
            Словарь, представляющий созданное примечание, или None в случае ошибки.
        """
        payload = [
            {
                "entity_id": lead_id,
                "note_type": "common",
                "params": {
                    "text": text
                }
            }
        ]
        try:
            response = await self._request('POST', '/leads/notes', json_data=payload)
            if response and '_embedded' in response and 'notes' in response['_embedded'] and response['_embedded']['notes']:
                created_note = response['_embedded']['notes'][0]
                logger.info(f"Примечание успешно добавлено к сделке ID {lead_id}.")
                return created_note
            return None
        except Exception as e:
            logger.error(f"Ошибка при добавлении примечания к сделке ID {lead_id}: {e}", exc_info=True)
            return None


    async def create_task(
        self,
        entity_id: int,
        responsible_user_id: int,
        text: str,
        complete_till_timestamp: int,
        entity_type: str = "leads",
        task_type_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Создает новую задачу в amoCRM для указанной сущности.
        Args:
            entity_id: ID сущности (сделки, контакта, компании), к которой привязана задача.
            responsible_user_id: ID ответственного пользователя.
            text: Текст задачи.
            complete_till_timestamp: Время завершения задачи в формате Unix timestamp.
            entity_type: Тип сущности ("leads", "contacts", "companies"). По умолчанию "leads".
            task_type_name: Имя типа задачи. Если None, будет использован тип по умолчанию из settings.
        Returns:
            Словарь, представляющий созданную задачу, или None в случае ошибки или некорректных входных данных.
        """
        if not all([entity_id, responsible_user_id, text, complete_till_timestamp]):
            logger.error("Недостаточно данных для создания задачи: entity_id, responsible_user_id, text, complete_till_timestamp должны быть заполнены.")
            return None
        
        payload_item: Dict[str, Any] = {
            "responsible_user_id": responsible_user_id,
            "entity_id": entity_id,
            "entity_type": entity_type,
            "text": text,
            "complete_till": complete_till_timestamp,
        }

        task_type_to_use_name = task_type_name if task_type_name else settings.TASK_TYPE_NAME_DEFAULT
        task_type_id = self.task_types_ids.get(task_type_to_use_name)

        if task_type_id:
            payload_item["task_type_id"] = task_type_id
        else:
            logger.warning(f"Тип задачи '{task_type_to_use_name}' не найден по имени в справочнике ID. Задача будет создана без явного указания типа. Возможно, AmoCRM применит тип по умолчанию.")
            
        payload = [payload_item]
        try:
            async with self._rate_limit:
                response = await self._request('POST', "/tasks", json_data=payload)
            
            if response and '_embedded' in response and 'tasks' in response['_embedded']:
                created_task = response['_embedded']['tasks'][0]
                logger.info(f"Задача ID {created_task.get('id')} успешно создана для {entity_type} ID {entity_id}.")
                return created_task
            else:
                logger.error(f"Неожиданный ответ при создании задачи: {response}")
                return None
        except ClientResponseError as e:
            logger.error(f"Ошибка при создании задачи для {entity_type} ID {entity_id}: {e.status}, message='{e.message}', url='{e.url}'")
            return None
        except Exception as e:
            logger.error(f"Неизвестная ошибка при создании задачи для {entity_type} ID {entity_id}: {e}", exc_info=True)
            return None


    async def get_linked_companies_to_lead(self, lead_id: int) -> List[Dict[str, Any]]:
        """
        Получает список компаний, связанных со сделкой.
        Args:
            lead_id: ID сделки.
        Returns:
            Список словарей, представляющих связанные компании.
        """
        endpoint = f"/leads/{lead_id}"
        try:
            response = await self._request('GET', endpoint, params={'with': 'companies'})
            if response and '_embedded' in response and 'companies' in response['_embedded']:
                return response['_embedded']['companies']
            return []
        except Exception as e:
            logger.error(f"Ошибка при получении связанных компаний для сделки ID {lead_id}: {e}", exc_info=True)
            return []


    async def link_company_to_lead(self, lead_id: int, company_id: int) -> bool:
        """
        Привязывает компанию к сделке.
        Args:
            lead_id: ID сделки.
            company_id: ID компании.
        Returns:
            True, если привязка успешна, False в противном случае.
        """
        payload = [
            {
                "id": lead_id,
                "_embedded": {
                    "companies": [
                        {"id": company_id}
                    ]
                }
            }
        ]
        try:
            response = await self._request('PATCH', '/leads', json_data=payload)
            return response is not None
        except Exception as e:
            logger.error(f"Ошибка при привязке компании ID {company_id} к сделке ID {lead_id}: {e}", exc_info=True)
            return False