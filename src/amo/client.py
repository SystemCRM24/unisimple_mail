import logging
from typing import Self, Optional, List, Dict, Any

from aiohttp import ClientSession, ClientResponseError
from aiolimiter import AsyncLimiter

from src.settings import settings

logger = logging.getLogger(__name__)

STATUS_NAME_AKKREDITACIYA = "Аккредитация"
STATUS_NAME_UCHASTNIKI = "Участники"
STATUS_NAME_KHOLODNYE_ZAYAVKI = "Холодные заявки"
STATUS_NAME_PERVICHNYE_PEREGOVORY = "Первичные переговоры"
STATUS_NAME_PEREGOVORY_LPR = "Переговоры с ЛПР"
STATUS_NAME_PEREGOVORY_NEW = "Переговоры"
CUSTOM_FIELD_NAME_INN = "ИНН"
CUSTOM_FIELD_NAME_PURCHASE_LINK = "Ссылка на закупку"
USER_NAME_ALENA = "Алена"
USER_NAME_NOVIKOVA_EVGENIYA = "Новикова Евгения"

PIPELINE_NAME_GOSZAKAZ = settings.PIPELINE_NAME_GOSZAKAZ
STATUS_NAME_POBEDITELI = settings.STATUS_NAME_POBEDITELI
CUSTOM_FIELD_NAME_INN_COMPANY = "ИНН"


class AmoClient:
    _session: ClientSession
    _MAX_REQUESTS_PER_SECOND = 2
    _API_VERSION = "v4"

    pipelines_ids: Dict[str, int] = {}
    statuses_ids: Dict[int, Dict[str, int]] = {}
    users_ids: Dict[str, int] = {}
    custom_fields_lead_ids: Dict[str, int] = {}
    custom_fields_company_ids: Dict[str, int] = {}

    def __init__(self):
        self._headers = {
            'Authorization': f'Bearer {settings.current_amo_long_term_token}',
            'Content-Type': 'application/json'
        }
        self._base_url = f"https://{settings.current_amo_subdomain}.amocrm.ru/api/{self._API_VERSION}"
        self._rate_limit = AsyncLimiter(self._MAX_REQUESTS_PER_SECOND, 1)
        self._initialized_ids = False

    async def __aenter__(self) -> Self:
        self._session = ClientSession(headers=self._headers, trust_env=True)
        await self._ensure_ids_initialized()
        return self

    async def __aexit__(self, *args) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        return

    async def _request(self, method: str, url: str, json_data: Optional[Dict[str, Any]] = None,
                       params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Метод для запросов к API amoCRM"""
        full_url = f"{self._base_url}{url}"
        async with self._rate_limit:
            try:
                kwargs = {}
                if json_data:
                    kwargs['json'] = json_data
                if params:
                    kwargs['params'] = params

                async with self._session.request(method, full_url, **kwargs) as response:
                    if 200 <= response.status < 300:
                        if response.status == 204:
                            return None
                        return await response.json()
                    else:
                        response_text = await response.text()
                        logger.error(
                            f"API request failed: {method} {full_url}, Status: {response.status}, Response: {response_text}, Request Data: {json_data}, Params: {params}")
                        response.raise_for_status()
                        return None
            except Exception as e:
                logger.error(f"Request to {full_url} failed: {e}", exc_info=True)
                return None

    async def _get_all_pages(self, endpoint: str, entity_key_in_embedded: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Вспомогательный метод для получения всех страниц данных из API."""
        all_data = []
        page = 1
        while True:
            current_params = params.copy() if params else {}
            current_params['page'] = page
            current_params['limit'] = 250

            try:
                response = await self._request('GET', endpoint, params=current_params)
            except ClientResponseError:
                logger.error(f"Failed to fetch page {page} for {endpoint} due to API error.")
                break

            if not response or '_embedded' not in response or entity_key_in_embedded not in response['_embedded']:
                break

            entities = response['_embedded'][entity_key_in_embedded]
            if not entities:
                break
            
            all_data.extend(entities)
            
            if '_links' in response and 'next' in response['_links']:
                page += 1
            else:
                break
        return all_data

    async def _ensure_ids_initialized(self):
        if self._initialized_ids:
            return
        logger.info("Инициализация ID из amoCRM...")
        try:
            pipelines_data = await self._get_all_pages('/leads/pipelines', 'pipelines')
            for p in pipelines_data:
                self.pipelines_ids[p['name']] = p['id']
                self.statuses_ids[p['id']] = {s['name']: s['id'] for s in p.get('_embedded', {}).get('statuses', [])}
                logger.info(f"Загружена воронка: '{p['name']}' (ID: {p['id']}) со статусами: {list(self.statuses_ids[p['id']].keys())}")

            users_data = await self._get_all_pages('/users', 'users')
            for u in users_data:
                self.users_ids[u['name']] = u['id']
            logger.info(f"Загружено {len(self.users_ids)} пользователей. Примеры: {list(self.users_ids.keys())[:3]}")

            lead_fields_data = await self._get_all_pages('/leads/custom_fields', 'custom_fields')
            for cf in lead_fields_data:
                self.custom_fields_lead_ids[cf['name']] = cf['id']
            logger.info(f"Загружено {len(self.custom_fields_lead_ids)} полей сделок. Примеры: {list(self.custom_fields_lead_ids.keys())[:3]}")
            
            company_fields_data = await self._get_all_pages('/companies/custom_fields', 'custom_fields')
            for cf in company_fields_data:
                self.custom_fields_company_ids[cf['name']] = cf['id']
            logger.info(f"Загружено {len(self.custom_fields_company_ids)} полей компаний. Примеры: {list(self.custom_fields_company_ids.keys())[:3]}")

            self._initialized_ids = True
            logger.info("Инициализация ID из amoCRM успешно завершена.")
        except Exception as e:
            logger.critical(f"Критическая ошибка при инициализации ID из amoCRM: {e}", exc_info=True)
            raise RuntimeError(f"Failed to initialize IDs from AmoCRM: {e}")

    async def get_pipeline_id(self, pipeline_name: str) -> Optional[int]:
        """Возвращает ID воронки по её имени."""
        return self.pipelines_ids.get(pipeline_name)

    async def get_status_id(self, pipeline_id: int, status_name: str) -> Optional[int]:
        """Возвращает ID этапа по ID воронки и имени этапа."""
        return self.statuses_ids.get(pipeline_id, {}).get(status_name)

    async def get_user_id(self, user_name: str) -> Optional[int]:
        """Возвращает ID пользователя по его имени."""
        return self.users_ids.get(user_name)

    async def get_custom_field_id(self, field_name: str) -> Optional[int]:
        """Возвращает ID пользовательского поля сделки по его имени."""
        return self.custom_fields_ids.get(field_name)

    async def get_company_custom_field_id(self, field_name: str) -> Optional[int]:
        """Возвращает ID пользовательского поля компании по его имени."""
        return self.custom_fields_company_ids.get(field_name)

    async def search_companies_by_inn(self, inn: str) -> List[Dict[str, Any]]:
        """
        Ищет компании по ИНН (пользовательское поле).
        Возвращает список найденных компаний.
        """
        inn_field_id = self.custom_fields_company_ids.get(CUSTOM_FIELD_NAME_INN)
        if not inn_field_id:
            logger.warning(f"Пользовательское поле '{CUSTOM_FIELD_NAME_INN}' (ИНН) не найдено для компаний. Поиск по ИНН невозможен.")
            return []

        params = {
            'query': inn,
            'with': 'custom_fields'
        }
        companies = await self._get_all_pages('/companies', params=params)

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

    async def create_company(self, name: str, inn: str) -> Optional[Dict[str, Any]]:
        """
        Создает новую компанию.
        """
        inn_field_id = self.custom_fields_company_ids.get(CUSTOM_FIELD_NAME_INN)
        if not inn_field_id:
            logger.error(f"Не удалось создать компанию: пользовательское поле '{CUSTOM_FIELD_NAME_INN}' (ИНН) не найдено для компаний.")
            return None

        data = [{
            "name": name,
            "custom_fields_values": [
                {
                    "field_id": inn_field_id,
                    "values": [{"value": inn}]
                }
            ]
        }]
        try:
            response = await self._request('POST', '/companies', json_data=data)
            return response['_embedded']['companies'][0] if response and '_embedded' in response and 'companies' in response['_embedded'] else None
        except ClientResponseError as e:
            logger.error(f"Ошибка API AmoCRM при создании компании (HTTP {e.status}): {e.message}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Неизвестная ошибка при создании компании: {e}", exc_info=True)
            return None

    async def search_leads_by_name(self, lead_name: str, pipeline_id: int, excluded_user_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """
        Ищет сделки по названию в указанной воронке, исключая определенных ответственных.
        """
        params = {
            'query': lead_name,
            'filter[pipeline_id]': pipeline_id,
            'with': 'contacts'
        }
        all_leads_by_query = await self._get_all_pages('/leads', entity_key_in_embedded='leads', params=params)
        
        filtered_leads = []
        for lead in all_leads_by_query:
            if lead.get('name') != lead_name:
                continue

            if excluded_user_ids:
                if lead.get('responsible_user_id') not in excluded_user_ids:
                    filtered_leads.append(lead)
            else:
                filtered_leads.append(lead)
        
        if filtered_leads:
             logger.debug(f"Найдено {len(filtered_leads)} сделок по имени '{lead_name}' после фильтрации.")
        else:
             logger.debug(f"Сделки по имени '{lead_name}' после фильтрации не найдены.")
        return filtered_leads

    async def create_lead(self, name: str, price: float, pipeline_id: int, status_id: int,
                          company_inn: Optional[str] = None, responsible_user_id: Optional[int] = None,
                          custom_fields: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Создает новую сделку.
        custom_fields: словарь вида {field_name_from_settings: value} для пользовательских полей.
        """

        company_id: Optional[int] = None

        if name and company_inn:
            created_company = await self.create_company(name=name, inn=str(company_inn))
            if created_company:
                company_id = created_company.get('id')
                logger.info(f"Компания ID {company_id} успешно создана и будет привязана к сделке '{name}'.")
            else:
                logger.warning(f"Не удалось создать компанию для сделки '{name}' (ИНН: {company_inn}). Сделка будет создана без привязки к компании.")
        elif name and not company_inn:
            logger.warning(f"Отсутствует ИНН для сделки '{name}'. Компания не будет создана и привязана.")
        else:
            logger.warning("Отсутствует имя или ИНН для создания компании при создании сделки.")

        lead_data: Dict[str, Any] = {
            "name": name,
            "pipeline_id": pipeline_id,
            "status_id": status_id
        }
        if company_id:
            lead_data["_embedded"] = {"companies": [{"id": company_id}]}
        if price is not None:
            lead_data["price"] = int(price) 
        if responsible_user_id:
            lead_data["responsible_user_id"] = responsible_user_id

        if custom_fields:
            cf_values_payload = []
            for field_name_key, value in custom_fields.items():
                # field_name_key - это ключ из settings, например settings.CUSTOM_FIELD_NAME_INN_LEAD
                # Его значение - это фактическое имя поля в AmoCRM, например "ИНН"
                # self.custom_fields_lead_ids хранит { "ИНН": id_поля_инн }
                field_id = self.custom_fields_lead_ids.get(field_name_key) # Получаем ID по имени поля
                if field_id:
                    cf_values_payload.append({
                        "field_id": field_id,
                        "values": [{"value": value}]
                    })
                else:
                    logger.warning(f"ID для кастомного поля сделки '{field_name_key}' не найден. Поле не будет установлено.")
            if cf_values_payload:
                lead_data["custom_fields_values"] = cf_values_payload
        
        payload = [lead_data]
        try:
            response = await self._request('POST', '/leads', json_data=payload)
            if response and '_embedded' in response and 'leads' in response['_embedded'] and response['_embedded']['leads']:
                return response['_embedded']['leads'][0]
            else:
                logger.error(f"Не удалось создать сделку '{name}'. Ответ API: {response}")
                return None
        except Exception as e:
            logger.error(f"Исключение при создании сделки '{name}': {e}", exc_info=True)
            return None

    async def add_note_to_lead(self, lead_id: int, note_text: str, note_type: str = "common") -> Optional[Dict[str, Any]]:
        """
        Добавляет текстовое примечание к сделке.

        :param lead_id: ID сделки.
        :param note_text: Текст примечания.
        :param note_type: Тип примечания (например, 'common', 'service_message').
        :return: Словарь с данными созданного примечания или None в случае ошибки.
        """

        if not lead_id or not note_text:
            logger.warning("Для добавления примечания необходимы lead_id и note_text.")
            return None

        payload = [{
            "note_type": note_type,
            "params": {
                "text": note_text
            }
        }]
        
        endpoint = f"/leads/{lead_id}/notes"
        try:
            response = await self._request('POST', endpoint, json_data=payload)
            if response and '_embedded' in response and 'notes' in response['_embedded'] and response['_embedded']['notes']:
                logger.info(f"Примечание успешно добавлено к сделке ID {lead_id}.")
                return response['_embedded']['notes'][0]
            else:
                logger.error(f"Не удалось добавить примечание к сделке ID {lead_id}. Ответ API: {response}")
                return None
        except ClientResponseError as e:
            logger.error(f"Ошибка API AmoCRM при добавлении примечания к сделке ID {lead_id} (HTTP {e.status}): {e.message}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Неизвестная ошибка при добавлении примечания к сделке ID {lead_id}: {e}", exc_info=True)
            return None
        
    async def get_lead_notes(self, lead_id: int, note_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if not lead_id: return []
        endpoint = f"/leads/{lead_id}/notes"
        params: Dict[str, Any] = {}
        if note_types:

            params["filter[note_type]"] = note_types 

        try:
            notes = await self._get_all_pages(endpoint, entity_key_in_embedded='notes', params=params)
            return notes
        except Exception as e:
            logger.error(f"Ошибка при получении примечаний для сделки ID {lead_id}: {e}", exc_info=True)
            return []
            
    async def update_lead(self, lead_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Обновляет сделку.
        :param lead_id: ID сделки для обновления.
        :param payload: Словарь с полями для обновления.
                        Например, {"price": 1000, "name": "Новое имя", "updated_at": timestamp}
        :return: Обновленная сделка или None.
        """
        if not lead_id or not payload:
            logger.warning("Для обновления сделки необходимы lead_id и payload.")
            return None

        endpoint = f"/leads/{lead_id}"
        try:
            response = await self._request('PATCH', endpoint, json_data=payload)
            if response and response.get('id') == lead_id:
                logger.info(f"Сделка ID {lead_id} успешно обновлена.")
                return response
            else:
                logger.error(f"Не удалось обновить сделку ID {lead_id}. Ответ API: {response}")
                return None
        except ClientResponseError as e:
            logger.error(f"Ошибка API AmoCRM при обновлении сделки ID {lead_id} (HTTP {e.status}): {e.message}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Неизвестная ошибка при обновлении сделки ID {lead_id}: {e}", exc_info=True)
            return None
    
    async def create_task_for_lead(self, lead_id: int, responsible_user_id: int, text: str, 
                                   complete_till_timestamp: int, task_type_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Создает задачу для сделки.
        :param lead_id: ID сделки.
        :param responsible_user_id: ID ответственного за задачу.
        :param text: Текст задачи.
        :param complete_till_timestamp: UNIX timestamp срока выполнения.
        :param task_type_id: ID типа задачи (если не указан, используется тип по умолчанию).
        :return: Созданная задача или None.
        """
        if not all([lead_id, responsible_user_id, text, complete_till_timestamp]):
            logger.warning("Для создания задачи не хватает обязательных параметров.")
            return None

        payload_item: Dict[str, Any] = {
            "responsible_user_id": responsible_user_id,
            "entity_id": lead_id,
            "entity_type": "leads",
            "text": text,
            "complete_till": complete_till_timestamp,
        }
        if task_type_id:
            payload_item["task_type_id"] = task_type_id
        else:
            # Можно получить ID дефолтного типа задачи "Связаться с клиентом" при инициализации
            default_task_type_id = self.task_types_ids.get("Связаться с клиентом") # Пример
            if default_task_type_id:
                 payload_item["task_type_id"] = default_task_type_id
            else: # Если не нашли, API сам выберет тип по умолчанию или можно жестко задать ID (например, 1)
                logger.debug("ID типа задачи по умолчанию не найден, task_type_id не будет установлен.")


        payload = [payload_item]
        endpoint = "/tasks"
        try:
            response = await self._request('POST', endpoint, json_data=payload)
            if response and '_embedded' in response and 'tasks' in response['_embedded'] and response['_embedded']['tasks']:
                logger.info(f"Задача для сделки ID {lead_id} успешно создана.")
                return response['_embedded']['tasks'][0]
            else:
                logger.error(f"Не удалось создать задачу для сделки ID {lead_id}. Ответ API: {response}")
                return None
        except Exception as e: # Ловим ClientResponseError и другие
            logger.error(f"Ошибка при создании задачи для сделки ID {lead_id}: {e}", exc_info=True)
            return None