import logging
from typing import Self, Optional, List, Dict, Any

from aiohttp import ClientSession, ClientResponseError
from aiolimiter import AsyncLimiter

from src.settings import settings

logger = logging.getLogger(__name__)


class AmoClient:
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
        self._session = ClientSession(headers=self._headers, trust_env=True)
        await self._ensure_ids_initialized()
        return self

    async def __aexit__(self, *args) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(self, method: str, url: str, json_data: Optional[Dict[str, Any]] = None,
                       params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
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

            self.task_types_ids = {}
            logger.info("Загрузка типов задач пропущена (эндпоинт /api/v4/tasks/types недоступен).")

            self._initialized_ids = True
            logger.info("Инициализация ID из amoCRM (кроме типов задач) успешно завершена.")
        except Exception as e:
            logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА при инициализации ID из amoCRM: {e}", exc_info=True)
            raise RuntimeError(f"Failed to initialize IDs from AmoCRM: {e}")

    async def get_pipeline_id(self, pipeline_name: str) -> Optional[int]:
        return self.pipelines_ids.get(pipeline_name)

    async def get_status_id(self, pipeline_id: int, status_name: str) -> Optional[int]:
        return self.statuses_ids.get(pipeline_id, {}).get(status_name)

    async def get_user_id(self, user_name: str) -> Optional[int]:
        return self.users_ids.get(user_name)

    async def get_custom_field_id_lead(self, field_name: str) -> Optional[int]:
        return self.custom_fields_lead_ids.get(field_name)

    async def get_custom_field_id_company(self, field_name: str) -> Optional[int]:
        return self.custom_fields_company_ids.get(field_name)

    async def get_task_type_id(self, task_type_name: str) -> Optional[int]:
        return self.task_types_ids.get(task_type_name)

    async def search_companies_by_inn(self, inn: str) -> List[Dict[str, Any]]:
        """
        Ищет компании по ИНН (пользовательское поле).
        Возвращает список найденных компаний.
        """
        inn_field_id = self.custom_fields_company_ids.get(settings.CUSTOM_FIELD_NAME_INN_COMPANY)
        if not inn_field_id:
            logger.warning(f"Пользовательское поле '{settings.CUSTOM_FIELD_NAME_INN_COMPANY}' (ИНН) не найдено для компаний. Поиск по ИНН невозможен.")
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

    async def create_company(self, name: str, inn: Optional[str] = None) -> Optional[Dict[str, Any]]:
        company_data: Dict[str, Any] = {"name": name}
        cf_values_payload = []
        if inn:
            inn_field_id = self.custom_fields_company_ids.get(settings.CUSTOM_FIELD_NAME_INN_COMPANY) 
            if inn_field_id:
                cf_values_payload.append({"field_id": inn_field_id, "values": [{"value": str(inn)}]})
            else:
                logger.warning(f"ID для поля ИНН '{settings.CUSTOM_FIELD_NAME_INN_COMPANY}' компании не найден. ИНН не будет установлен для '{name}'.")

        if cf_values_payload:
            company_data["custom_fields_values"] = cf_values_payload

        payload_list = [company_data]
        try:
            response = await self._request('POST', '/companies', json_data=payload_list)
            if response and '_embedded' in response and 'companies' in response['_embedded'] and response['_embedded']['companies']:
                created_company = response['_embedded']['companies'][0]
                logger.info(f"Создана компания '{name}' (ID: {created_company.get('id')}).")
                return created_company
            return None
        except Exception:
            return None

    async def search_leads_by_name(self, pipeline_id: int, purchase_number:str, excluded_user_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        params = {'query': purchase_number, 'filter[pipeline_id]': pipeline_id, 'with': 'responsible_user'}
        all_leads = await self._get_all_pages('/leads', entity_key_in_embedded='leads', params=params)

        purchase_number_field_id = self.custom_fields_lead_ids.get(settings.CUSTOM_FIELD_NAME_PURCHASE_LINK_LEAD)
        filtered_leads = []
        for lead in all_leads:
            if 'custom_fields_values' in lead:
                for custom_field in lead['custom_fields_values']:
                    if custom_field['field_id'] == purchase_number_field_id:
                        for value in custom_field['values']:
                            if str(value['value']).split()[0] == purchase_number:
                                filtered_leads.append(lead)
                                break
            if excluded_user_ids and lead.get('responsible_user_id') in excluded_user_ids: continue
        return filtered_leads

    async def get_lead_details(self, lead_id: int, with_relations: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        if not lead_id:
            return None
        endpoint = f"/leads/{lead_id}"
        params = {}
        if with_relations:
            params['with'] = ",".join(with_relations)
        try:
            return await self._request('GET', endpoint, params=params)
        except Exception:
            return None

    async def create_lead(self, name: str, price: float, pipeline_id: int, status_id: int,
                          company_inn: Optional[str] = None, 
                          responsible_user_id: Optional[int] = None,
                          custom_fields: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        company_id_to_link: Optional[int] = None
        if company_inn:
            logger.info(f"Создание НОВОЙ компании для сделки '{name}' с ИНН: {company_inn} (согласно ТЗ).")
            created_company = await self.create_company(name=name, inn=str(company_inn))
            if created_company:
                company_id_to_link = created_company.get('id')
            else:
                logger.warning(f"Не удалось создать НОВУЮ компанию для сделки '{name}' (ИНН: {company_inn}). Сделка будет создана без привязки к компании.")

        lead_data: Dict[str, Any] = {
            "name": name, "price": int(price),
            "pipeline_id": pipeline_id, "status_id": status_id,
        }
        if responsible_user_id:
            lead_data["responsible_user_id"] = responsible_user_id

        cf_values_payload = []
        if custom_fields:
            for field_name_from_settings, value in custom_fields.items():
                field_id = self.custom_fields_lead_ids.get(field_name_from_settings) 
                if field_id:
                    cf_values_payload.append({"field_id": field_id, "values": [{"value": str(value)}]})
                else:
                    logger.warning(f"ID для custom_fields сделки '{field_name_from_settings}' не найден. Поле не будет установлено.")
        if cf_values_payload:
            lead_data["custom_fields_values"] = cf_values_payload

        _embedded_data = {}
        if company_id_to_link:
            _embedded_data["companies"] = [{"id": company_id_to_link}]
        if _embedded_data:
            lead_data["_embedded"] = _embedded_data

        payload_list = [lead_data]
        try:
            response = await self._request('POST', '/leads', json_data=payload_list)
            if response and '_embedded' in response and 'leads' in response['_embedded'] and response['_embedded']['leads']:
                return response['_embedded']['leads'][0]
            return None
        except Exception:
            return None

    async def add_note_to_lead(self, lead_id: int, note_text: str, note_type: str = "common") -> Optional[Dict[str, Any]]:
        if not lead_id or not note_text:
            return None
        payload = [{"note_type": note_type, "params": {"text": note_text}}]
        try:
            response = await self._request('POST', f"/leads/{lead_id}/notes", json_data=payload)
            if response and '_embedded' in response and 'notes' in response['_embedded'] and response['_embedded']['notes']:
                return response['_embedded']['notes'][0]
            return None
        except Exception:
            return None

    async def update_lead(self, lead_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not lead_id or not payload:
            return None
        try:
            response = await self._request('PATCH', f"/leads/{lead_id}", json_data=payload)
            if response and response.get('id') == lead_id:
                return response
            return None
        except Exception:
            return None

    async def create_task_for_lead(self, lead_id: int, responsible_user_id: int, text: str, 
                                   complete_till_timestamp: int, task_type_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if not all([lead_id, responsible_user_id, text, complete_till_timestamp]):
            return None
        payload_item: Dict[str, Any] = {
            "responsible_user_id": responsible_user_id, "entity_id": lead_id,
            "entity_type": "leads", "text": text, "complete_till": complete_till_timestamp,
        }

        task_type_to_use_name = task_type_name if task_type_name else settings.TASK_TYPE_NAME_DEFAULT
        task_type_id = self.task_types_ids.get(task_type_to_use_name)

        if task_type_id:
            payload_item["task_type_id"] = task_type_id
        else:
            logger.warning(f"Тип задачи '{task_type_to_use_name}' не найден по имени. API использует тип по умолчанию (или может вернуть ошибку, если тип обязателен и дефолта нет).")

        payload = [payload_item]
        try:
            response = await self._request('POST', "/tasks", json_data=payload)
            if response and '_embedded' in response and 'tasks' in response['_embedded'] and response['_embedded']['tasks']:
                return response['_embedded']['tasks'][0]
            return None
        except Exception:
            return None
