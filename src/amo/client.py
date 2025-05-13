import logging
from typing import Self, Optional, List, Dict, Any

from aiohttp import ClientSession, ClientResponseError
from aiolimiter import AsyncLimiter

from src.settings import settings

logger = logging.getLogger(__name__)

PIPELINE_NAME_GOSZAKAZ = "Гос.заказ - прогрев клиента"
STATUS_NAME_POBEDITELI = "Победители"
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


class AmoClient:
    _session: ClientSession
    _MAX_REQUESTS_PER_SECOND = 2
    _API_VERSION = "v4"

    pipelines_ids: Dict[str, int] = {}
    statuses_ids: Dict[int, Dict[str, int]] = {}
    users_ids: Dict[str, int] = {}
    custom_fields_ids: Dict[str, int] = {}
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

    async def _get_all_pages(self, url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Вспомогательный метод для получения всех страниц данных из API."""
        all_data = []
        page = 1
        while True:
            current_params = params.copy() if params else {}
            current_params['page'] = page
            response = await self._request('GET', url, params=current_params)
            if not response or '_embedded' not in response:
                break

            entity_key = None
            for key in ['leads', 'companies', 'users', 'pipelines', 'custom_fields']:
                if key in response['_embedded']:
                    entity_key = key
                    break

            if not entity_key:
                logger.warning(f"No recognizable entity key in response for {url}. Keys found: {response['_embedded'].keys()}")
                break

            entities = response['_embedded'][entity_key]
            if not entities:
                break
            all_data.extend(entities)
            page += 1
            if '_links' in response and 'next' in response['_links']:
                pass
            else:
                break
        return all_data

    async def _ensure_ids_initialized(self):
        """
        Инициализирует ID воронок, этапов, пользователей и пользовательских полей
        при первом входе в контекстный менеджер или при необходимости.
        """
        if self._initialized_ids:
            return

        logger.info("Инициализация ID из amoCRM...")

        logger.debug("Запрос воронок и этапов...")
        pipelines_response = await self._request('GET', '/leads/pipelines')
        if pipelines_response and '_embedded' in pipelines_response and 'pipelines' in pipelines_response['_embedded']:
            for p in pipelines_response['_embedded']['pipelines']:
                self.pipelines_ids[p['name']] = p['id']
                self.statuses_ids[p['id']] = {}
                logger.info(f"Найдена воронка: '{p['name']}' (ID: {p['id']})")
                if '_embedded' in p and 'statuses' in p['_embedded']:
                    for s in p['_embedded']['statuses']:
                        self.statuses_ids[p['id']][s['name']] = s['id']
                        logger.info(f"  Этап для '{p['name']}': '{s['name']}' (ID: {s['id']})")
                else:
                    logger.warning(f"  Нет статусов для воронки '{p['name']}' (ID: {p['id']}).")
            
            if PIPELINE_NAME_GOSZAKAZ not in self.pipelines_ids:
                logger.error(f"Воронка '{PIPELINE_NAME_GOSZAKAZ}' не найдена в amoCRM. Доступные воронки: {list(self.pipelines_ids.keys())}")
            else:
                target_pipeline_id = self.pipelines_ids[PIPELINE_NAME_GOSZAKAZ]
                logger.debug(f"Статусы для воронки '{PIPELINE_NAME_GOSZAKAZ}' (ID: {target_pipeline_id}): {list(self.statuses_ids.get(target_pipeline_id, {}).keys())}")
                if STATUS_NAME_POBEDITELI not in self.statuses_ids.get(target_pipeline_id, {}):
                    logger.error(f"Этап '{STATUS_NAME_POBEDITELI}' не найден в воронке '{PIPELINE_NAME_GOSZAKAZ}' (ID: {target_pipeline_id}). Проверьте название этапа в amoCRM.")

        logger.debug("Запрос пользователей...")
        users = await self._get_all_pages('/users')
        if users:
            for u in users:
                self.users_ids[u['name']] = u['id']
                logger.info(f"Пользователь: '{u['name']}' (ID: {u['id']})")
        else:
            logger.warning("Не удалось получить список пользователей.")

        logger.debug("Запрос пользовательских полей для сделок...")
        custom_fields_leads = await self._request('GET', '/leads/custom_fields')
        if custom_fields_leads and '_embedded' in custom_fields_leads and 'custom_fields' in custom_fields_leads['_embedded']:
            for cf in custom_fields_leads['_embedded']['custom_fields']:
                self.custom_fields_ids[cf['name']] = cf['id']
                logger.info(f"Кастомное поле сделки: '{cf['name']}' (ID: {cf['id']})")
        else:
            logger.warning("Не удалось получить список пользовательских полей сделок.")

        logger.debug("Запрос пользовательских полей для компаний...")
        custom_fields_companies = await self._request('GET', '/companies/custom_fields')
        if custom_fields_companies and '_embedded' in custom_fields_companies and 'custom_fields' in custom_fields_companies['_embedded']:
            found_company_fields = [cf['name'] for cf in custom_fields_companies['_embedded']['custom_fields']]
            logger.debug(f"Получены пользовательские поля компаний: {found_company_fields}")
            if CUSTOM_FIELD_NAME_INN not in found_company_fields:
                logger.warning(f"Пользовательское поле '{CUSTOM_FIELD_NAME_INN}' не найдено среди полей компаний в amoCRM. Найдено: {found_company_fields}")
            for cf in custom_fields_companies['_embedded']['custom_fields']:
                self.custom_fields_company_ids[cf['name']] = cf['id']
                logger.info(f"Кастомное поле компании: '{cf['name']}' (ID: {cf['id']})")
        else:
            logger.warning("Не удалось получить список пользовательских полей компаний.")
            logger.debug(f"Полный ответ API для /companies/custom_fields: {custom_fields_companies}")

        self._initialized_ids = True
        logger.info("Инициализация ID завершена.")

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
        Возвращает список найденных сделок.
        """

        params = {
            'query': lead_name,
            'filter[pipeline_id]': pipeline_id,
        }

        leads = await self._get_all_pages('/leads', params=params)

        filtered_leads = []
        if excluded_user_ids:
            for lead in leads:
                if lead.get('responsible_user_id') not in excluded_user_ids:
                    if lead.get('name') == lead_name:
                        filtered_leads.append(lead)
        else:
            for lead in leads:
                if lead.get('name') == lead_name:
                    filtered_leads.append(lead)
        return filtered_leads

    async def create_lead(self, name: str, price: float, pipeline_id: int, status_id: int,
                          company_id: Optional[int] = None, responsible_user_id: Optional[int] = None,
                          custom_fields: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Создает новую сделку.
        custom_fields: словарь вида {field_name: value} для пользовательских полей.
        """

        lead_data = {
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
            cf_values = []
            for field_name, value in custom_fields.items():
                field_id = self.custom_fields_ids.get(field_name)
                if field_id:
                    cf_values.append({
                        "field_id": field_id,
                        "values": [{"value": value}]
                    })
                else:
                    logger.warning(f"Неизвестное пользовательское поле для сделки: '{field_name}'")
            if cf_values:
                lead_data["custom_fields_values"] = cf_values

        data = [lead_data]
        try:
            response = await self._request('POST', '/leads', json_data=data)
            if response and '_embedded' in response and 'leads' in response['_embedded']:
                return response['_embedded']['leads'][0]
            else:
                logger.error(f"Не удалось создать сделку. Ответ API: {response}")
                return None
        except ClientResponseError as e:
            logger.error(f"Ошибка API AmoCRM при создании сделки (HTTP {e.status}): {e.message}. Подробности: {e.request_info.url}, {e.history}, {e.headers}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Произошла необработанная ошибка при создании сделки: {e}", exc_info=True)
            return None