# src/amo/client.py
import logging
from typing import Self, Optional, List, Dict, Any

# Используем ClientResponseError напрямую
from aiohttp import ClientSession, ClientResponseError as AioHttpClientResponseError 
from aiolimiter import AsyncLimiter

from src.settings import settings # settings теперь содержит все нужные имена

logger = logging.getLogger(__name__)

class AmoClient:
    _session: ClientSession
    # Если settings.request_delay = 0.5 (секунд на запрос), то это 1/0.5 = 2 запроса в секунду.
    _MAX_REQUESTS_PER_SECOND: float = 1.0 / settings.request_delay if settings.request_delay > 0 else 2.0
    _API_VERSION = "v4"

    pipelines_ids: Dict[str, int]
    statuses_ids: Dict[int, Dict[str, int]]
    users_ids: Dict[str, int]
    custom_fields_lead_ids: Dict[str, int] # Имя поля -> ID
    custom_fields_company_ids: Dict[str, int] # Имя поля -> ID
    task_types_ids: Dict[str, int]

    def __init__(self):
        self._headers = {
            'Authorization': f'Bearer {settings.current_amo_long_term_token}',
            'Content-Type': 'application/json'
        }
        self._base_url = f"https://{settings.current_amo_subdomain}.amocrm.ru/api/{self._API_VERSION}"
        self._rate_limit = AsyncLimiter(max_rate=self._MAX_REQUESTS_PER_SECOND, period=1)
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

    async def _request(self, method: str, endpoint: str, json_data: Optional[Dict[str, Any]] = None,
                       params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        full_url = f"{self._base_url}{endpoint}"
        async with self._rate_limit:
            try:
                logger.debug(f"AmoAPI Request: {method} {full_url} | Params: {params} | JSON: {json_data is not None}")
                async with self._session.request(method, full_url, json=json_data, params=params) as response:
                    logger.debug(f"AmoAPI Response Status: {response.status} for {full_url}")
                    if 200 <= response.status < 300:
                        if response.status == 204: return None
                        return await response.json()
                    else:
                        response_body = await response.text()
                        logger.error(
                            f"API request error: {method} {full_url}, Status: {response.status}, "
                            f"Response Body: {response_body[:500]}..."
                        )
                        response.raise_for_status()
                        return None 
            except AioHttpClientResponseError as e:
                logger.error(f"ClientResponseError for {method} {full_url}: {e.status} {e.message}")
                raise 
            except Exception as e:
                logger.error(f"Unexpected error during request to {full_url}: {e}", exc_info=True)
                raise

    async def _get_all_pages(self, endpoint: str, entity_key_in_embedded: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        all_data: List[Dict[str, Any]] = []
        page = 1
        while True:
            current_params = params.copy() if params else {}
            current_params['page'] = page
            current_params['limit'] = 250
            try:
                response = await self._request('GET', endpoint, params=current_params)
            except Exception: # Ловим все ошибки от _request (включая ClientResponseError)
                logger.error(f"API error or unexpected error fetching page {page} for {endpoint}. Stopping pagination.", exc_info=True)
                break

            if not response or '_embedded' not in response or entity_key_in_embedded not in response['_embedded']:
                if page == 1 and response and '_embedded' in response and not response['_embedded'].get(entity_key_in_embedded):
                    logger.debug(f"No entities '{entity_key_in_embedded}' found on first page for {endpoint}.")
                elif page > 1:
                     logger.debug(f"No more entities '{entity_key_in_embedded}' on page {page} for {endpoint}.")
                else:
                    logger.warning(f"Unexpected response structure or no '{entity_key_in_embedded}' for {endpoint} on page {page}. Response: {str(response)[:200]}...")
                break
            
            entities = response['_embedded'][entity_key_in_embedded]
            if not isinstance(entities, list):
                if entities: all_data.append(entities)
            else:
                if not entities: break
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
                logger.info(f"Воронка: '{p['name']}' (ID: {p['id']}), Статусы: {list(self.statuses_ids[p['id']].keys())}")

            users_data = await self._get_all_pages('/users', 'users')
            for u in users_data: self.users_ids[u['name']] = u['id']
            logger.info(f"Загружено {len(self.users_ids)} пользователей.")

            lead_fields_data = await self._get_all_pages('/leads/custom_fields', 'custom_fields')
            for cf in lead_fields_data: self.custom_fields_lead_ids[cf['name']] = cf['id']
            logger.info(f"Загружено {len(self.custom_fields_lead_ids)} полей сделок.")
            
            company_fields_data = await self._get_all_pages('/companies/custom_fields', 'custom_fields')
            for cf in company_fields_data: self.custom_fields_company_ids[cf['name']] = cf['id']
            logger.info(f"Загружено {len(self.custom_fields_company_ids)} полей компаний.")

            task_types_data = await self._get_all_pages('/tasks/types', 'task_types') # entity_key для /tasks/types это 'task_types'
            for tt in task_types_data: self.task_types_ids[tt['name']] = tt['id']
            logger.info(f"Загружено {len(self.task_types_ids)} типов задач.")

            self._initialized_ids = True
            logger.info("Инициализация ID из amoCRM успешно завершена.")
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
        # Имя поля ИНН для компаний берется из settings
        inn_field_name_from_settings = settings.CUSTOM_FIELD_NAME_INN_COMPANY
        inn_field_id = self.custom_fields_company_ids.get(inn_field_name_from_settings)
        
        if not inn_field_id:
            logger.warning(f"Поле ИНН '{inn_field_name_from_settings}' для компаний не найдено. Поиск по ИНН невозможен.")
            return []
        
        # Фильтр по кастомному полю (field_id) и его значению (inn)
        # GET /api/v4/companies?filter[custom_fields_values][{field_id}]={inn_value}
        # Это не поддерживается параметром 'query' напрямую.
        # Мы должны получить компании и отфильтровать, либо использовать более сложный запрос.
        # Пока что используем query, но это может быть неточно и вернуть лишние компании.
        # Затем фильтруем по ИНН на клиенте.
        params = {'query': inn, 'with': 'custom_fields_values'} # Запрашиваем значения полей
        all_companies_by_query = await self._get_all_pages('/companies', entity_key_in_embedded='companies', params=params)
        
        found_companies = []
        for company in all_companies_by_query:
            cf_values = company.get('custom_fields_values')
            if cf_values:
                for cf in cf_values:
                    if cf.get('field_id') == inn_field_id:
                        for val_entry in cf.get('values', []):
                            if str(val_entry.get('value', '')).strip() == str(inn).strip():
                                found_companies.append(company)
                                break # Нашли совпадение в этом поле
                        break # Перешли к следующей компании
        logger.debug(f"По ИНН '{inn}' найдено {len(found_companies)} компаний.")
        return found_companies

    async def create_company(self, name: str, inn: Optional[str] = None) -> Optional[Dict[str, Any]]:
        company_data: Dict[str, Any] = {"name": name}
        cf_values_payload = []
        if inn:
            inn_field_name = settings.CUSTOM_FIELD_NAME_INN_COMPANY
            inn_field_id = self.custom_fields_company_ids.get(inn_field_name)
            if inn_field_id:
                cf_values_payload.append({"field_id": inn_field_id, "values": [{"value": str(inn)}]})
            else:
                logger.warning(f"Не удалось найти ID поля ИНН '{inn_field_name}' для компании. ИНН не будет установлен для '{name}'.")
        
        if cf_values_payload:
            company_data["custom_fields_values"] = cf_values_payload
        
        payload_list = [company_data]
        try:
            response = await self._request('POST', '/companies', json_data=payload_list)
            if response and '_embedded' in response and 'companies' in response['_embedded'] and response['_embedded']['companies']:
                created_company = response['_embedded']['companies'][0]
                logger.info(f"Компания '{name}' (ID: {created_company.get('id')}) успешно создана.")
                return created_company
            return None
        except Exception: return None # Ошибки логируются в _request

    async def search_leads_by_name(self, lead_name: str, pipeline_id: int, excluded_user_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        params = {'query': lead_name, 'filter[pipeline_id]': pipeline_id, 'with': 'responsible_user'} # 'with' для получения деталей ответственного
        all_leads = await self._get_all_pages('/leads', entity_key_in_embedded='leads', params=params)
        
        filtered_leads = []
        for lead in all_leads:
            if lead.get('name', '').strip().lower() != lead_name.strip().lower(): # Сравнение без учета регистра и пробелов
                continue
            if excluded_user_ids and lead.get('responsible_user_id') in excluded_user_ids:
                continue
            filtered_leads.append(lead)
        logger.debug(f"Найдено {len(filtered_leads)} подходящих сделок для '{lead_name}'.")
        return filtered_leads

    async def create_lead(self, name: str, price: float, pipeline_id: int, status_id: int,
                          company_id: Optional[int] = None, 
                          responsible_user_id: Optional[int] = None,
                          custom_fields_payload: Optional[List[Dict[str,Any]]] = None) -> Optional[Dict[str, Any]]:
        lead_data: Dict[str, Any] = {
            "name": name, "price": int(price),
            "pipeline_id": pipeline_id, "status_id": status_id,
        }
        if responsible_user_id: lead_data["responsible_user_id"] = responsible_user_id
        if custom_fields_payload: lead_data["custom_fields_values"] = custom_fields_payload
        
        _embedded_data = {}
        if company_id: _embedded_data["companies"] = [{"id": company_id}]
        if _embedded_data: lead_data["_embedded"] = _embedded_data
        
        payload_list = [lead_data]
        try:
            response = await self._request('POST', '/leads', json_data=payload_list)
            if response and '_embedded' in response and 'leads' in response['_embedded'] and response['_embedded']['leads']:
                return response['_embedded']['leads'][0]
            return None
        except Exception: return None

    async def add_note_to_lead(self, lead_id: int, note_text: str, note_type: str = "common") -> Optional[Dict[str, Any]]:
        if not lead_id or not note_text: return None
        payload = [{"note_type": note_type, "params": {"text": note_text}}]
        endpoint = f"/leads/{lead_id}/notes"
        try:
            response = await self._request('POST', endpoint, json_data=payload)
            if response and '_embedded' in response and 'notes' in response['_embedded'] and response['_embedded']['notes']:
                return response['_embedded']['notes'][0]
            return None
        except Exception: return None
            
    async def update_lead(self, lead_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not lead_id or not payload: return None
        endpoint = f"/leads/{lead_id}"
        try:
            response = await self._request('PATCH', endpoint, json_data=payload)
            if response and response.get('id') == lead_id: return response
            return None
        except Exception: return None
    
    async def create_task_for_lead(self, lead_id: int, responsible_user_id: int, text: str, 
                                   complete_till_timestamp: int, task_type_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if not all([lead_id, responsible_user_id, text, complete_till_timestamp]): return None
        payload_item: Dict[str, Any] = {
            "responsible_user_id": responsible_user_id, "entity_id": lead_id,
            "entity_type": "leads", "text": text, "complete_till": complete_till_timestamp,
        }
        if task_type_name:
            task_type_id = self.task_types_ids.get(task_type_name)
            if task_type_id: payload_item["task_type_id"] = task_type_id
            else: logger.warning(f"Тип задачи '{task_type_name}' не найден, будет использован тип по умолчанию API.")
        
        payload = [payload_item]
        try:
            response = await self._request('POST', "/tasks", json_data=payload)
            if response and '_embedded' in response and 'tasks' in response['_embedded'] and response['_embedded']['tasks']:
                return response['_embedded']['tasks'][0]
            return None
        except Exception: return None

    async def get_lead_details(self, lead_id: int, with_relations: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """Получает детали сделки, включая возможные связи."""
        if not lead_id: return None
        endpoint = f"/leads/{lead_id}"
        params = {}
        if with_relations:
            params['with'] = ",".join(with_relations) # e.g., "contacts,companies"
        try:
            return await self._request('GET', endpoint, params=params)
        except Exception:
            return None