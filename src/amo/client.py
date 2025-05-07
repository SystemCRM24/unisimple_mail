import logging
from typing import Self, Optional, Dict, Any, List

from aiohttp import ClientSession
from aiolimiter import AsyncLimiter

from src.settings import settings
from src.utils.http_client import HTTPClient

logger = logging.getLogger(__name__)


class AmoClient:
    """
    Клиент для взаимодействия с API amoCRM, использующий HTTPClient для выполнения запросов.
    """
    _BASE_URL_V4 = f"https://{settings.amo_subdomain}.amocrm.ru/api/v4"
    _MAX_REQUESTS_PER_SECOND = 2

    def __init__(self):
        """
        Инициализация AmoClient. Создает и настраивает внутренний HTTPClient.
        """
        amo_headers = {
            'Authorization': f'Bearer {settings.amo_long_term_token}',
            'Content-Type': 'application/json'
        }
        rate_limiter = AsyncLimiter(self._MAX_REQUESTS_PER_SECOND, 1)
        self._http_client = HTTPClient(
            base_url=self._BASE_URL_V4,
            headers=amo_headers,
            rate_limiter=rate_limiter
        )
        logger.debug("AmoClient initialized with internal HTTPClient.")

    async def __aenter__(self) -> Self:
        """
        Вход в асинхронный контекстный менеджер.
        Инициализирует внутренний HTTPClient.
        """
        await self._http_client.__aenter__()
        logger.debug("AmoClient entered async context.")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Выход из асинхронного контекстного менеджера.
        Закрывает внутренний HTTPClient.
        """
        await self._http_client.__aexit__(exc_type, exc_val, exc_tb)
        logger.debug("AmoClient exited async context.")
        return False

    # --- Методы для работы с сущностями amoCRM --- 

    async def get_pipelines(self) -> Optional[List[Dict[str, Any]]]:
        """
        Получает список всех воронок.
        """
        response = await self._http_client.get('/leads/pipelines')
        if response and '_embedded' in response and 'pipelines' in response['_embedded']:
            return response['_embedded']['pipelines']
        return None

    async def get_users(self) -> Optional[List[Dict[str, Any]]]:
        """
        Получает список всех пользователей аккаунта.
        """
        response = await self._http_client.get('/users')
        if response and '_embedded' in response and 'users' in response['_embedded']:
            return response['_embedded']['users']
        return None

    async def get_leads(self, pipeline_id: Optional[int] = None, status_id: Optional[int] = None, responsible_user_id: Optional[int] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Получает список сделок с возможностью фильтрации.

        Args:
            pipeline_id: ID воронки для фильтрации.
            status_id: ID этапа воронки для фильтрации.
            responsible_user_id: ID ответственного пользователя для фильтрации.
        """
        params = {}
        if pipeline_id is not None:
            params['filter[pipeline_id]'] = pipeline_id
        if status_id is not None:
            params['filter[status_id]'] = status_id
        if responsible_user_id is not None:
            params['filter[responsible_user_id]'] = responsible_user_id

        all_leads = []
        page = 1
        limit = 250
        while True:
            current_params = params.copy()
            current_params['page'] = page
            current_params['limit'] = limit
            response = await self._http_client.get('/leads', params=current_params)

            if response and '_embedded' in response and 'leads' in response['_embedded']:
                leads = response['_embedded']['leads']
                if not leads:
                    break
                all_leads.extend(leads)

                if '_links' not in response or 'next' not in response['_links']:
                    break

                page += 1
            else:
                if response is not None and '_embedded' in response and 'leads' in response['_embedded'] and not response['_embedded']['leads']:
                    break
                logger.error("Ошибка при получении списка сделок или данные отсутствуют в ожидаемом формате.")
                return all_leads if all_leads else None

        return all_leads

    async def delete_lead(self, lead_id: int) -> bool:
        """
        Удаляет сделку по ID.

        Args:
            lead_id: ID сделки для удаления.

        Returns:
            True, если удаление успешно (статус 204), иначе False.
        """
        response = await self._http_client.delete(f'/leads/{lead_id}')
        return response is None

    async def update_lead(self, lead_id: int, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Обновляет данные сделки по ID.

        Args:
            lead_id: ID сделки для обновления.
            data: Словарь с данными для обновления (например, {'status_id': 123}).

        Returns:
            Словарь с обновленными данными сделки при успехе, иначе None.
        """
        update_data = [{'id': lead_id, **data}]
        response = await self._http_client.patch('/leads', json=update_data)
        if response and '_embedded' in response and 'leads' in response['_embedded']:
            return response['_embedded']['leads'][0] if response['_embedded']['leads'] else None
        return None

    async def update_leads_batch(self, data: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
        """
        Обновляет несколько сделок пачкой.

        Args:
            data: Список словарей с данными для обновления каждой сделки.
                  Каждый словарь должен содержать как минимум {'id': <lead_id>, ...}.

        Returns:
            Список словарей с обновленными данными сделок при успехе, иначе None.
        """
        response = await self._http_client.patch('/leads', json=data)
        if response and '_embedded' in response and 'leads' in response['_embedded']:
            return response['_embedded']['leads']
        return None

    async def add_note_to_lead(self, lead_id: int, note_text: str) -> Optional[Dict[str, Any]]:
        """
        Добавляет примечание к сделке.

        Args:
            lead_id: ID сделки.
            note_text: Текст примечания.

        Returns:
            Словарь с данными созданного примечания при успехе, иначе None.
        """
        note_data = [
            {
                "entity_id": lead_id,
                "note_type": "standard",
                "params": {
                    "text": note_text
                }
            }
        ]
        response = await self._http_client.post(f'/leads/{lead_id}/notes', json=note_data)
        if response and '_embedded' in response and 'notes' in response['_embedded']:
            return response['_embedded']['notes'][0] if response['_embedded']['notes'] else None
        return None

    async def create_task(self, lead_id: int, task_type_id: int, text: str, complete_till: int, responsible_user_id: int) -> Optional[Dict[str, Any]]:
        """
        Создает задачу для сделки.

        Args:
            lead_id: ID сделки.
            task_type_id: ID типа задачи (например, 1 - Звонок, 2 - Встреча).
            text: Текст задачи.
            complete_till: Время до дедлайна в формате Unix Timestamp.
            responsible_user_id: ID ответственного пользователя задачи.

        Returns:
            Словарь с данными созданной задачи при успехе, иначе None.
        """
        task_data = [
            {
                "entity_id": lead_id,
                "entity_type": "leads",
                "task_type_id": task_type_id,
                "text": text,
                "complete_till": complete_till,
                "responsible_user_id": responsible_user_id
            }
        ]
        response = await self._http_client.post('/tasks', json=task_data)
        if response and '_embedded' in response and 'tasks' in response['_embedded']:
            return response['_embedded']['tasks'][0] if response['_embedded']['tasks'] else None
        return None

    async def get_contacts(self, query: Optional[str] = None, responsible_user_id: Optional[int] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Получает список контактов с возможностью поиска или фильтрации по ответственному.

        Args:
            query: Строка поиска по контактам (например, имя, телефон, email).
            responsible_user_id: ID ответственного пользователя для фильтрации.
        """
        params = {}
        if query:
            params['query'] = query
        if responsible_user_id is not None:
            params['filter[responsible_user_id]'] = responsible_user_id

        if not params:
            logger.warning("Calling get_contacts without query or responsible_user_id might fetch a large number of contacts.")

        all_contacts = []
        page = 1
        limit = 250
        while True:
            current_params = params.copy()
            current_params['page'] = page
            current_params['limit'] = limit
            response = await self._http_client.get('/contacts', params=current_params)
            if response and '_embedded' in response and 'contacts' in response['_embedded']:
                contacts = response['_embedded']['contacts']
                if not contacts:
                    break
                all_contacts.extend(contacts)
                if '_links' not in response or 'next' not in response['_links']:
                    break
                page += 1
            else:
                if response is not None and '_embedded' in response and 'contacts' in response['_embedded'] and not response['_embedded']['contacts']:
                    break
                logger.error("Ошибка при получении списка контактов или данные отсутствуют в ожидаемом формате.")
                return all_contacts if all_contacts else None

        return all_contacts

    async def get_companies(self, query: Optional[str] = None, responsible_user_id: Optional[int] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Получает список компаний с возможностью поиска или фильтрации по ответственному.

        Args:
            query: Строка поиска по компаниям (например, название, ИНН).
            responsible_user_id: ID ответственного пользователя для фильтрации.
        """
        params = {}
        if query:
            params['query'] = query
        if responsible_user_id is not None:
            params['filter[responsible_user_id]'] = responsible_user_id

        if not params:
            logger.warning("Calling get_companies without query or responsible_user_id might fetch a large number of companies.")

        all_companies = []
        page = 1
        limit = 250
        while True:
            current_params = params.copy()
            current_params['page'] = page
            current_params['limit'] = limit
            response = await self._http_client.get('/companies', params=current_params)
            if response and '_embedded' in response and 'companies' in response['_embedded']:
                companies = response['_embedded']['companies']
                if not companies:
                    break
                all_companies.extend(companies)
                if '_links' not in response or 'next' not in response['_links']:
                    break
                page += 1
            else:
                if response is not None and '_embedded' in response and 'companies' in response['_embedded'] and not response['_embedded']['companies']:
                    break
                logger.error("Ошибка при получении списка компаний или данные отсутствуют в ожидаемом формате.")
                return all_companies if all_companies else None

        return all_companies

    async def create_company(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Создает новую компанию.

        Args:
            data: Словарь с данными компании (например, {'name': 'Название компании', 'custom_fields_values': [...]}).

        Returns:
            Словарь с данными созданной компании при успехе, иначе None.
        """
        company_data = [data]
        response = await self._http_client.post('/companies', json=company_data)
        if response and '_embedded' in response and 'companies' in response['_embedded']:
            return response['_embedded']['companies'][0] if response['_embedded']['companies'] else None
        return None

    async def create_contact(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Создает новый контакт.

        Args:
            data: Словарь с данными контакта (например, {'name': 'Иван Иванов', 'custom_fields_values': [...]}).

        Returns:
            Словарь с данными созданного контакта при успехе, иначе None.
        """
        contact_data = [data]
        response = await self._http_client.post('/contacts', json=contact_data)
        if response and '_embedded' in response and 'contacts' in response['_embedded']:
            return response['_embedded']['contacts'][0] if response['_embedded']['contacts'] else None
        return None

    async def create_lead(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Создает новую сделку.

        Args:
            data: Словарь с данными сделки (например, {'name': 'Новая сделка', 'price': 1000}).

        Returns:
            Словарь с данными созданной сделки при успехе, иначе None.
        """
        lead_data = [data]
        response = await self._http_client.post('/leads', json=lead_data)
        if response and '_embedded' in response and 'leads' in response['_embedded']:
            return response['_embedded']['leads'][0] if response['_embedded']['leads'] else None
        return None

    async def link_lead_to_company(self, lead_id: int, company_id: int) -> bool:
        """
        Привязывает сделку к компании.

        Args:
            lead_id: ID сделки.
            company_id: ID компании.

        Returns:
            True при успехе, иначе False.
        """
        link_data = [
            {
                "id": lead_id,
                "company": {
                    "id": company_id
                }
            }
        ]
        response = await self._http_client.patch('/leads', json=link_data)
        return response is not None

    async def link_lead_to_contact(self, lead_id: int, contact_id: int) -> bool:
        """
        Привязывает сделку к контакту.

        Args:
            lead_id: ID сделки.
            contact_id: ID контакта.

        Returns:
            True при успехе, иначе False.
        """
        link_data = [
            {
                "id": lead_id,
                "contacts": {
                    "attach": [
                        {"id": contact_id}
                    ]
                }
            }
        ]
        response = await self._http_client.patch('/leads', json=link_data)
        return response is not None

    async def get_custom_fields(self, entity_type: str) -> Optional[List[Dict[str, Any]]]:
        """
        Получает список пользовательских полей для указанной сущности.

        Args:
            entity_type: Тип сущности ('leads', 'contacts', 'companies').

        Returns:
            Список словарей с данными полей при успехе, иначе None.
        """
        response = await self._http_client.get(f'/{entity_type}/custom_fields')
        if response and '_embedded' in response and 'custom_fields' in response['_embedded']:
            return response['_embedded']['custom_fields']
        return None

    async def get_pipelines_with_statuses(self) -> Optional[List[Dict[str, Any]]]:
        """
        Получает список всех воронок с их этапами.
        """
        response = await self._http_client.get('/leads/pipelines')
        if response and '_embedded' in response and 'pipelines' in response['_embedded']:
            return response['_embedded']['pipelines']
        return None

    async def update_pipeline_status(self, pipeline_id: int, status_id: int, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Обновляет данные этапа воронки.

        Args:
            pipeline_id: ID воронки.
            status_id: ID этапа.
            data: Словарь с данными для обновления (например, {'name': 'Новое название'}).

        Returns:
            Словарь с обновленными данными этапа при успехе, иначе None.
        """
        update_data = [data]
        response = await self._http_client.patch(f'/leads/pipelines/{pipeline_id}/statuses/{status_id}', json=update_data)
        if response and '_embedded' in response and 'statuses' in response['_embedded']:
            return response['_embedded']['statuses'][0] if response['_embedded']['statuses'] else None
        return None


    async def delete_pipeline_status(self, pipeline_id: int, status_id: int) -> bool:
        """
        Удаляет этап воронки.

        Args:
            pipeline_id: ID воронки.
            status_id: ID этапа для удаления.

        Returns:
            True, если удаление успешно (статус 204), иначе False.
        """
        response = await self._http_client.delete(f'/leads/pipelines/{pipeline_id}/statuses/{status_id}')
        return response is None