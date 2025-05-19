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
        f"Обеспечение контракта: {format_value(purchase_data.contract_securing)}",
        f"Обеспечение гарантийных обязательств: {format_value(purchase_data.warranty_obligations_securing)}",
        f"Окончание контракта: {format_value(purchase_data.contract_end_date)}",
        f"Цена победителя: {format_value(purchase_data.winner_price)}"
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
    lead_info_for_task: Dict[str, Any],
    is_new_lead: bool,
    purchase_number_for_task: str,
    id_user_anastasia_popova: Optional[int],
    id_user_unsorted: Optional[int]
):
    """
    Создает задачу в amoCRM для указанной сделки.

    Определяет ответственного за задачу исходя из того, является ли сделка новой
    и назначен ли у нее ответственный менеджер или статус "Неразобранное".

    Args:
        amo_client: Экземпляр клиента AmoClient.
        lead_id: ID сделки, к которой привязывается задача.
        lead_info_for_task: Словарь с информацией о сделке.
        is_new_lead: Флаг, указывающий, является ли сделка новой (только что созданной).
        purchase_number_for_task: Номер закупки для включения в текст задачи.
        id_user_anastasia_popova: ID пользователя "Анастасия Попова".
        id_user_unsorted: ID пользователя "Неразобранные заявки".
    Returns:
        None.
    """
    task_responsible_id: Optional[int] = None
    task_text = f"{settings.TASK_TEXT_NEW_TENDER_WIN}"
    
    actual_lead_responsible_id = lead_info_for_task.get('responsible_user_id')

    if is_new_lead:
        if actual_lead_responsible_id == id_user_unsorted or actual_lead_responsible_id is None:
            if id_user_anastasia_popova:
                task_responsible_id = id_user_anastasia_popova
                logger.info(f"Новая сделка ID {lead_id} не разобрана/ответственный по умолчанию. Задача на '{settings.USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA}'.")
            else:
                logger.warning(f"Новая сделка ID {lead_id} не разобрана, но ID '{settings.USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA}' не найден. Задача не поставлена.")
                return
        elif actual_lead_responsible_id:
            task_responsible_id = actual_lead_responsible_id
            logger.info(f"Новая сделка ID {lead_id} с ответственным ID {actual_lead_responsible_id}. Задача на него.")
        else:
            if id_user_anastasia_popova:
                task_responsible_id = id_user_anastasia_popova
                logger.warning(f"Ответственный для новой сделки ID {lead_id} не определен. Задача будет поставлена на '{settings.USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA}'.")
            else:
                return
    else:
        if actual_lead_responsible_id and actual_lead_responsible_id != id_user_unsorted:
            task_responsible_id = actual_lead_responsible_id
            logger.info(f"Новая победа для существующей сделки ID {lead_id} с ответственным ID {actual_lead_responsible_id}. Задача на него.")
        else:
            if id_user_anastasia_popova:
                task_responsible_id = id_user_anastasia_popova
                logger.info(f"Новая победа для существующей сделки ID {lead_id}, но ответственный не проставлен/неразобран. Задача на '{settings.USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA}'.")
            else:
                logger.warning(f"Ответственный для существующей сделки ID {lead_id} не проставлен, и ID '{settings.USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA}' не найден. Задача не поставлена.")
                return

    if task_responsible_id:
        complete_till = int((datetime.now(timezone.utc) + timedelta(minutes=settings.TASK_COMPLETE_OFFSET_MINUTES)).timestamp())

        created_task = await amo_client.create_task_for_lead(
            lead_id, task_responsible_id, task_text, complete_till,
            task_type_name=settings.TASK_TYPE_NAME_DEFAULT
        )
        if created_task:
            logger.info(f"Задача для сделки ID {lead_id} (текст: '{task_text[:50]}...') успешно поставлена на пользователя ID {task_responsible_id}.")
        else:
            logger.error(f"Не удалось поставить задачу для сделки ID {lead_id} на пользователя ID {task_responsible_id}.")


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

    if purchase_data.inn and exclude_user_ids_for_creation_filter:
        existing_companies = await amo_client.search_companies_by_inn(str(purchase_data.inn))
        for comp in existing_companies:
            if comp.get('responsible_user_id') in exclude_user_ids_for_creation_filter:
                logger.info(f"Победитель '{deal_name}' (ИНН: {purchase_data.inn}) через компанию '{comp.get('name')}' (ID: {comp.get('id')}) уже закреплен за исключенным менеджером. Новая сделка создаваться не будет.")
                return

    found_leads = await amo_client.search_leads_by_name(
        pipeline_id=pipeline_id,
        excluded_user_ids=exclude_user_ids_for_creation_filter,
        purchase_number=purchase_data.purchase_number
    )

    lead_info_for_task: Dict[str, Any] = {"name": deal_name, "responsible_user_id": None}

    if found_leads:
        lead_info = found_leads[0]
        current_lead_id = lead_info.get('id')
        lead_current_responsible_id = lead_info.get('responsible_user_id')
        lead_current_budget = float(lead_info.get('price', 0.0))
        lead_info_for_task["responsible_user_id"] = lead_current_responsible_id
        logger.info(f"Найдена существующая сделка: '{deal_name}' (ID: {current_lead_id}, отв.: {lead_current_responsible_id}, бюджет: {lead_current_budget})")

        new_budget_from_excel = purchase_data.contract_securing
        if new_budget_from_excel is not None:
            new_budget_float = float(new_budget_from_excel)
            payload_update_lead: Dict[str, Any] = {}
            if new_budget_float != 0.0:
                if new_budget_float != lead_current_budget:
                    payload_update_lead["price"] = int(new_budget_float)

            if payload_update_lead:
                budget_changed_during_update = True
    else:
        is_new_lead = True
        logger.info(f"Сделка для '{deal_name}' не найдена. Создание новой.")

        custom_fields_for_creation: Dict[str, Any] = {}
        if purchase_data.inn:
            custom_fields_for_creation[settings.CUSTOM_FIELD_NAME_INN_LEAD] = str(purchase_data.inn)

        purchase_link_value = ""
        if purchase_data.purchase_number and purchase_data.eis_url:
            purchase_link_value = f"{purchase_data.purchase_number} {purchase_data.eis_url}"
        elif purchase_data.eis_url:
            purchase_link_value = purchase_data.eis_url
        elif purchase_data.purchase_number:
            purchase_link_value = f"Номер закупки: {purchase_data.purchase_number}"

        if purchase_link_value:
            custom_fields_for_creation[settings.CUSTOM_FIELD_NAME_PURCHASE_LINK_LEAD] = purchase_link_value

        price_for_amo = float(purchase_data.contract_securing) if purchase_data.contract_securing is not None else 0.0

        created_lead_data = await amo_client.create_lead(
            name=deal_name, price=price_for_amo, pipeline_id=pipeline_id, status_id=target_status_id,
            company_inn=str(purchase_data.inn) if purchase_data.inn else None,
            custom_fields=custom_fields_for_creation if custom_fields_for_creation else None,
            responsible_user_id=id_user_unsorted 
        )

        if created_lead_data:
            current_lead_id = created_lead_data.get('id')
            lead_current_responsible_id = created_lead_data.get('responsible_user_id') 
            lead_info_for_task["responsible_user_id"] = lead_current_responsible_id
            lead_current_budget = price_for_amo
            logger.info(f"Сделка '{deal_name}' (ID: {current_lead_id}) успешно создана. Ответственный (по умолчанию): {lead_current_responsible_id}")
        else:
            logger.error(f"Не удалось создать сделку '{deal_name}'.")
            return

    if current_lead_id:
        note_text = generate_note_text_for_win(purchase_data)
        added_note = await amo_client.add_note_to_lead(current_lead_id, note_text)

        payload_for_final_update: Dict[str, Any] = {"updated_at": int(datetime.now(timezone.utc).timestamp())}
        if budget_changed_during_update and "price" in locals().get("payload_update_lead", {}):
            payload_for_final_update["price"] = locals()["payload_update_lead"]["price"]

        await amo_client.update_lead(current_lead_id, payload_for_final_update)
        if "price" in payload_for_final_update:
            logger.info(f"Сделка ID {current_lead_id} обновлена (бюджет и/или updated_at) и перемещена вверх.")
        else:
            logger.info(f"Сделка ID {current_lead_id} перемещена вверх (updated_at).")

        await _create_task(
            amo_client, current_lead_id,
            lead_info_for_task,
            is_new_lead, str(purchase_data.purchase_number),
            id_user_anastasia_popova, id_user_unsorted
        )


async def process_parsed_data_for_amocrm(
    amo_client: AmoClient,
    parsed_purchases: List[DBStatePurchase]
):
    """
    Основная функция для обработки списка данных о выигранных закупках и синхронизации их с amoCRM.

    Инициализирует необходимые справочники (воронки, статусы, пользователи, поля),
    затем итерируется по каждой записи о закупке и вызывает функцию обработки
    отдельной сделки (_handle_lead_processing).

    Args:
        amo_client: Экземпляр клиента AmoClient.
        parsed_purchases: Список объектов DBStatePurchase, содержащих данные о выигранных закупках.
    Returns:
        None.
    """
    logger.info(f"--- Запуск обработки {len(parsed_purchases)} записей для amoCRM ---")
    try:
        await amo_client._ensure_ids_initialized()
    except RuntimeError:
        logger.critical("Сбой инициализации ID amoCRM. Обработка остановлена.")
        return

    exclude_user_ids_for_filter = []
    if settings.EXCLUDE_RESPONSIBLE_USERS:
        for user_name in settings.EXCLUDE_RESPONSIBLE_USERS:
            user_id = await amo_client.get_user_id(user_name) 
            if user_id:
                exclude_user_ids_for_filter.append(user_id)
            else:
                logger.warning(f"Пользователь '{user_name}' для исключения не найден.")

    pipeline_id = await amo_client.get_pipeline_id(settings.PIPELINE_NAME_GOSZAKAZ)
    if not pipeline_id:
        logger.error(f"Воронка '{settings.PIPELINE_NAME_GOSZAKAZ}' не найдена."); return

    target_status_id = await amo_client.get_status_id(pipeline_id, settings.STATUS_NAME_POBEDITELI)
    if not target_status_id: 
        logger.error(f"Этап '{settings.STATUS_NAME_POBEDITELI}' в воронке '{settings.PIPELINE_NAME_GOSZAKAZ}' не найден."); return

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
                exclude_user_ids_for_filter, 
                id_anastasia_popova, 
                id_unsorted_leads
            )
        except Exception as e:
            logger.error(f"Критическая ошибка при полной обработке записи '{getattr(purchase_data, 'purchase_number', 'N/A')}': {e}", exc_info=True)

    logger.info(f"--- Обработка для amoCRM {len(parsed_purchases)} записей завершена ---")