import logging
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timezone, timedelta

from src.amo.client import AmoClient
from src.amo.schemas import DBStatePurchase
from src.settings import settings

logger = logging.getLogger(__name__)


def format_value(value: Any) -> str:
    """
    Форматирует значение для отображения в примечании.

    Args:
        value: Входное значение любого типа.
    Returns:
        Отформатированная строка ('не указано' для None, дата в формате dd.mm.yyyy,
        числа без десятичных знаков если целое или с двумя знаками после запятой,
        остальные типы преобразуются в строку).
    """
    if value is None:
        return "не указано"
    if isinstance(value, date): return value.strftime('%d.%m.%Y')
    if isinstance(value, (float, int)):
        return str(int(value)) if float(value).is_integer() else f"{float(value):.2f}"
    return str(value)


def format_number_with_spaces(number_str: str) -> str:
    if not number_str.isdigit():
        raise ValueError("Входная строка должна содержать только цифры")

    reversed_str = number_str[::-1]
    chunks = [reversed_str[i:i+3] for i in range(0, len(reversed_str), 3)]

    formatted = ' '.join(chunks)[::-1]

    return formatted + " р."


def generate_note_text_for_win(purchase_data: DBStatePurchase) -> str:
    """
    Генерирует форматированный текст примечания для сделки о выигрыше в закупке.

    Args:
        purchase_data: Объект DBStatePurchase с данными о закупке.
    Returns:
        Многострочная строка, содержащая информацию о закупке и победителе.
    """
    note_lines = [
        f"Ссылка на закупку: {format_value(purchase_data.eis_url)}",
        f"Наименование победителя: {format_value(purchase_data.winner_name)}",
        f"ИНН: {format_value(purchase_data.inn)}",
        f"Дата итогов: {format_value(purchase_data.result_date)}",
        f"Наименование заказчика: {format_value(purchase_data.customer_name)}",
        f"НМЦК: {format_value(purchase_data.nmck)}",
        f"Обеспечение контракта: {format_number_with_spaces(format_value(purchase_data.contract_securing))}",
        f"Обеспечение гарантийных обязательств: {format_value(purchase_data.warranty_obligations_securing)}",
        f"Окончание контракта: {format_value(purchase_data.contract_end_date)}",
        f"Цена победителя: {format_number_with_spaces(format_value(purchase_data.winner_price))}"
    ]
    contact_details_lines = []
    for i in range(1, 4):
        fio = getattr(purchase_data, f'fio_{i}', None)
        phone = getattr(purchase_data, f'phone_{i}', None)
        email = getattr(purchase_data, f'email_{i}', None)
        if fio or phone or email:
            contact_details_lines.append(
                f"  - Контакт {i}: ФИО: {format_value(fio)}, Телефон: {format_value(phone)}, Email: {format_value(email)}"
            )
    if contact_details_lines:
        note_lines.append("Контактные данные:")
        note_lines.extend(contact_details_lines)
    else:
        note_lines.append("Контактные данные: не указаны")
    note_lines.extend([
        f"Преимущества СМП: {format_value(purchase_data.smp_advantages)}",
        f"Статус СМП: {format_value(purchase_data.smp_status)}"
    ])
    return "\n".join(note_lines)


async def _create_task(
    amo_client: AmoClient,
    lead_id: int,
    lead_info: Dict[str, Any],
    is_new_lead: bool,
    purchase_number: str,
    id_user_anastasia_popova: Optional[int],
    id_user_unsorted: Optional[int]
):
    """
    Создает задачу в AmoCRM для сделки.

    Args:
        amo_client: Экземпляр клиента AmoClient.
        lead_id: ID сделки, к которой привязана задача.
        lead_info: Словарь с информацией о сделке.
        is_new_lead: Флаг, указывающий, является ли сделка новой.
        purchase_number: Номер закупки для текста задачи.
        id_user_anastasia_popova: ID пользователя "Анастасия Попова".
        id_user_unsorted: ID пользователя "Неразобранное".
    Returns:
        None.
    """
    responsible_user_id = lead_info.get('responsible_user_id')
    task_text = f"Пришло обновление из базы победителей."
    complete_till_timestamp = int((datetime.now(timezone.utc) + timedelta(minutes=settings.TASK_COMPLETE_OFFSET_MINUTES)).timestamp())

    if not responsible_user_id:
        logger.warning(f"Для сделки ID {lead_id} не найден ответственный. Задача не будет создана.")
        return

    task_assigned_to_id = responsible_user_id
    if is_new_lead and responsible_user_id == id_user_unsorted and id_user_anastasia_popova:
        task_assigned_to_id = id_user_anastasia_popova
        logger.info(f"Сделка новая и в 'Неразобранных', задача будет назначена Анастасии Поповой (ID: {id_user_anastasia_popova}).")
    else:
        if responsible_user_id == id_user_unsorted or responsible_user_id is None:
            if id_user_anastasia_popova:
                task_assigned_to_id = id_user_anastasia_popova
                logger.info(f"Существующая сделка ID {lead_id}: ответственный 'Неразобранные заявки' или не проставлен. Задача на '{settings.USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA}'.")
            else:
                logger.warning(f"Существующая сделка ID {lead_id}: ответственный 'Неразобранные заявки' или не проставлен, но ID '{settings.USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA}' не найден. Задача не поставлена.")
                return
        elif responsible_user_id:
            task_assigned_to_id = responsible_user_id
            logger.info(f"Существующая сделка ID {lead_id}: ответственный ID {responsible_user_id}. Задача на него.")
        logger.info(f"Задача будет назначена текущему ответственному сделки (ID: {responsible_user_id}).")

    task_type_name = settings.TASK_TYPE_NAME_DEFAULT

    if await amo_client.create_task(
        entity_id=lead_id,
        responsible_user_id=task_assigned_to_id,
        text=task_text,
        complete_till_timestamp=complete_till_timestamp,
        entity_type="leads",
        task_type_name=task_type_name
    ):
        logger.info(f"Задача успешно создана для сделки ID {lead_id} и назначена пользователю ID {task_assigned_to_id}.")
    else:
        logger.error(f"Не удалось создать задачу для сделки ID {lead_id}.")


async def _handle_lead_processing(
    amo_client: AmoClient,
    purchase_data: DBStatePurchase,
    pipeline_id: int,
    target_status_id: int,
    exclude_user_ids_for_creation_filter: List[int],
    id_user_anastasia_popova: Optional[int],
    id_user_unsorted: Optional[int]
):
    """
    Обрабатывает одну запись о закупке: ищет существующую сделку, создает новую при необходимости,
    создает или находит компанию, привязывает компанию к сделке, добавляет примечание и создает задачу.

    Args:
        amo_client: Экземпляр клиента AmoClient.
        purchase_data: Объект DBStatePurchase с данными о закупке.
        pipeline_id: ID целевой воронки.
        target_status_id: ID целевого статуса в воронке.
        exclude_user_ids_for_creation_filter: Список ID пользователей, закрепленные компании за которыми
                                                 исключают создание новой сделки.
        id_user_anastasia_popova: ID пользователя "Анастасия Попова".
        id_user_unsorted: ID пользователя "Неразобранное".
    Returns:
        None.
    """
    current_lead_id: Optional[int] = None
    is_new_lead = False
    lead_current_responsible_id: Optional[int] = None
    lead_current_budget: Optional[float] = 0.0
    budget_changed_during_update = False

    if purchase_data.contract_securing is None or purchase_data.contract_securing < settings.MIN_LEAD_BUDGET:
        logger.debug(f"Пропуск (бюджет < {settings.MIN_LEAD_BUDGET}): '{purchase_data.winner_name}' ({purchase_data.purchase_number}), бюджет {purchase_data.contract_securing}")
        return

    deal_name = purchase_data.winner_name
    if not deal_name:
        logger.warning(f"Пропуск (нет имени победителя): закупка '{purchase_data.purchase_number}'")
        return

    logger.info(f"Обработка: '{deal_name}' (Закупка: {purchase_data.purchase_number}, ИНН: {purchase_data.inn})")

    company_id_to_link: Optional[int] = None
    company_responsible_user_id: Optional[int] = None

    if purchase_data.inn:
        found_companies = await amo_client.search_companies_by_inn(str(purchase_data.inn))
        if found_companies:
            company_info = found_companies[0]
            company_id_to_link = company_info.get('id')
            company_responsible_user_id = company_info.get('responsible_user_id')
            logger.info(f"Компания с ИНН '{purchase_data.inn}' найдена: '{company_info.get('name')}' (ID: {company_id_to_link}).")
            
            if company_responsible_user_id in exclude_user_ids_for_creation_filter:
                logger.info(f"Компания '{company_info.get('name')}' (ID: {company_id_to_link}) закреплена за исключенным менеджером ID {company_responsible_user_id}. Новая сделка не будет создаваться, обновление существующих также не будет.")
                return
        else:
            logger.info(f"Компания с ИНН '{purchase_data.inn}' не найдена. Создаем новую.")
            company_phones = [getattr(purchase_data, f'phone_{i}', None) for i in range(1, 4) if getattr(purchase_data, f'phone_{i}', None)]
            company_emails = [getattr(purchase_data, f'email_{i}', None) for i in range(1, 4) if getattr(purchase_data, f'email_{i}', None)]

            created_company = await amo_client.create_company(
                name=purchase_data.winner_name,
                inn=purchase_data.inn,
                phone_numbers=company_phones,
                emails=company_emails,
                responsible_user_id=id_user_unsorted
            )
            if created_company:
                company_id_to_link = created_company.get('id')
                company_responsible_user_id = created_company.get('responsible_user_id')
                logger.info(f"Новая компания '{purchase_data.winner_name}' (ID: {company_id_to_link}) создана с ответственным '{settings.USER_NAME_UNSORTED_LEADS}'.")
            else:
                logger.error(f"Не удалось создать компанию для '{purchase_data.winner_name}' (ИНН: {purchase_data.inn}).")
                return

    found_leads = await amo_client.search_leads_by_name(
        pipeline_id=pipeline_id,
        purchase_number=purchase_data.purchase_number
    )

    lead_info_for_task: Dict[str, Any] = {"name": deal_name}

    if found_leads:
        current_lead_id = found_leads[0].get('id')
        lead_info_for_task = found_leads[0]
        lead_current_responsible_id = found_leads[0].get('responsible_user_id')
        lead_current_budget = found_leads[0].get('price', 0.0)
        logger.info(f"Существующая сделка найдена: '{deal_name}' (ID: {current_lead_id}).")
        
        if purchase_data.contract_securing != lead_current_budget:
            budget_changed_during_update = True
            logger.info(f"Бюджет сделки ID {current_lead_id} изменился с {lead_current_budget} на {purchase_data.contract_securing}.")
    else:
        is_new_lead = True
        logger.info(f"Сделка '{deal_name}' не найдена. Создаем новую.")

        new_lead_responsible_id = company_responsible_user_id if company_responsible_user_id else id_user_unsorted

        created_lead = await amo_client.create_lead(
            name=deal_name,
            price=purchase_data.contract_securing,
            pipeline_id=pipeline_id,
            status_id=target_status_id,
            responsible_user_id=new_lead_responsible_id,
            company_id=company_id_to_link,
            custom_fields=[
                {"field_name": settings.CUSTOM_FIELD_NAME_INN_LEAD, "values": [str(purchase_data.inn)]},
                {"field_name": settings.CUSTOM_FIELD_NAME_PURCHASE_LINK_LEAD, "values": [purchase_data.eis_url]},
                {"field_name": settings.CUSTOM_FIELD_NAME_PURCHASE_NUMBER, "values": [purchase_data.purchase_number]},
                {"field_name": settings.CUSTOM_FIELD_NAME_TIME_ZONE, "values": [purchase_data.time_zone]}
            ]
        )
        if created_lead:
            current_lead_id = created_lead.get('id')
            lead_info_for_task = created_lead
            lead_current_responsible_id = created_lead.get('responsible_user_id')
            logger.info(f"Новая сделка '{deal_name}' (ID: {current_lead_id}) успешно создана с ответственным ID {lead_current_responsible_id}.")
        else:
            logger.error(f"Не удалось создать новую сделку для '{deal_name}'.")
            return

    if current_lead_id and not is_new_lead and budget_changed_during_update:
        logger.info(f"Обновляем бюджет сделки ID {current_lead_id} на {purchase_data.contract_securing}.")
        updated_lead = await amo_client.update_lead(
            lead_id=current_lead_id,
            price=purchase_data.contract_securing
        )
        if updated_lead:
            logger.info(f"Бюджет сделки ID {current_lead_id} успешно обновлен.")
        else:
            logger.error(f"Не удалось обновить бюджет сделки ID {current_lead_id}.")

    if current_lead_id and company_id_to_link:
        linked_companies = await amo_client.get_linked_companies_to_lead(current_lead_id)
        linked_company_ids = [comp.get('id') for comp in linked_companies]

        if company_id_to_link not in linked_company_ids:
            if await amo_client.link_company_to_lead(current_lead_id, company_id_to_link):
                logger.info(f"Компания ID {company_id_to_link} успешно привязана к сделке ID {current_lead_id}.")
            else:
                logger.error(f"Не удалось привязать компанию ID {company_id_to_link} к сделке ID {current_lead_id}.")
        else:
            logger.info(f"Компания ID {company_id_to_link} уже привязана к сделке ID {current_lead_id}. Пропуск привязки.")
    elif not company_id_to_link:
        logger.warning(f"Не удалось привязать компанию к сделке ID {current_lead_id}: company_id_to_link не определен.")

    if current_lead_id:
        note_text = generate_note_text_for_win(purchase_data)
        if await amo_client.add_note_to_lead(current_lead_id, note_text):
            logger.info(f"Примечание успешно добавлено к сделке ID {current_lead_id}.")
        else:
            logger.error(f"Не удалось добавить примечание к сделке ID {current_lead_id}.")

        await _create_task(
            amo_client,
            current_lead_id,
            lead_info_for_task,
            is_new_lead,
            purchase_data.purchase_number,
            id_user_anastasia_popova,
            id_user_unsorted
        )


async def process_parsed_data_for_amocrm(amo_client: AmoClient, parsed_purchases: List[DBStatePurchase]):
    """
    Основная функция для обработки распарсенных данных о закупках и синхронизации с amoCRM.

    Args:
        amo_client: Экземпляр клиента AmoClient.
        parsed_purchases: Список объектов DBStatePurchase с данными о закупках.
    Returns:
        None.
    """
    pipeline_id = await amo_client.get_pipeline_id(settings.PIPELINE_NAME_GOSZAKAZ)
    if not pipeline_id: 
        logger.error(f"Воронка '{settings.PIPELINE_NAME_GOSZAKAZ}' не найдена."); return

    target_status_id = await amo_client.get_status_id(pipeline_id, settings.STATUS_NAME_POBEDITELI)
    if not target_status_id: 
        logger.error(f"Этап '{settings.STATUS_NAME_POBEDITELI}' в воронке '{settings.PIPELINE_NAME_GOSZAKAZ}' не найден."); return

    exclude_user_ids_for_filter: List[int] = []
    for user_name in settings.EXCLUDE_RESPONSIBLE_USERS:
        user_id = await amo_client.get_user_id(user_name)
        if user_id:
            exclude_user_ids_for_filter.append(user_id)
        else:
            logger.warning(f"Пользователь '{user_name}' из списка исключений не найден в amoCRM. Игнорируется.")

    id_anastasia_popova = await amo_client.get_user_id(settings.USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA)
    id_unsorted_leads = await amo_client.get_user_id(settings.USER_NAME_UNSORTED_LEADS)

    if not id_anastasia_popova:
        logger.warning(f"ID пользователя '{settings.USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA}' (для задач) не найден. Логика задач может быть нарушена.")
    if not id_unsorted_leads:
        logger.warning(f"ID пользователя '{settings.USER_NAME_UNSORTED_LEADS}' не найден. Логика задач для неразобранных может быть нарушена.")

    for purchase_data in parsed_purchases:
        try:
            await _handle_lead_processing(
                amo_client, purchase_data, pipeline_id, target_status_id,
                exclude_user_ids_for_filter, id_anastasia_popova, id_unsorted_leads
            )
        except Exception as e:
            logger.error(f"Ошибка при обработке закупки '{purchase_data.purchase_number}': {e}", exc_info=True)