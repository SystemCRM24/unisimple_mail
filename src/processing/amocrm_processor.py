# src/processing/amocrm_processor.py
import logging
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timezone, timedelta

from src.amo.client import AmoClient
from src.amo.schemas import DBStatePurchase # Используем DBStatePurchase для доступа ко всем полям
from src.settings import settings

logger = logging.getLogger(__name__)

def format_value(value: Any) -> str:
    if value is None: return "не указано"
    if isinstance(value, date): return value.strftime('%d.%m.%Y')
    if isinstance(value, (float, int)): # Обработка и int, и float
        return str(int(value)) if float(value).is_integer() else f"{float(value):.2f}"
    return str(value)

def generate_note_text_for_win(purchase_data: DBStatePurchase) -> str:
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
    lead_responsible_id: Optional[int], # Текущий ответственный сделки
    is_new_lead: bool,
    purchase_winner_name: str,
    id_user_anastasia_popova: Optional[int],
    id_user_unsorted: Optional[int],
    purchase_data: DBStatePurchase
):
    """Создает задачу согласно ТЗ."""
    task_responsible_id: Optional[int] = None
    task_text: str = ""

    if not id_user_anastasia_popova:
        logger.warning(f"Не удалось получить ID пользователя '{settings.USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA}' для назначения задач. Задачи не будут созданы.")
        return

    actual_lead_responsible_id = lead_responsible_id
    if is_new_lead: # Для новых сделок, перепроверяем ответственного
        lead_details = await amo_client.get_lead_details(lead_id)
        if lead_details:
            actual_lead_responsible_id = lead_details.get('responsible_user_id')
            logger.info(f"Для новой сделки ID {lead_id} актуальный ответственный ID: {actual_lead_responsible_id}")
        else:
            logger.warning(f"Не удалось получить детали для новой сделки ID {lead_id} для определения ответственного.")
            # Если детали не получены, ставим задачу на Попову как на неразобранную
            actual_lead_responsible_id = id_user_unsorted # Предполагаем, что она неразобранная


    if actual_lead_responsible_id == id_user_unsorted or actual_lead_responsible_id is None:
        task_responsible_id = id_user_anastasia_popova
        task_text_template = settings.TASK_TEXT_NEW_LEAD_UNSORTED
        logger.info(f"Сделка ID {lead_id} не разобрана/ответственный 'НЕРАЗОБРАННЫЕ ЗАЯВКИ'. Задача на '{settings.USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA}'.")
    elif actual_lead_responsible_id:
        task_responsible_id = actual_lead_responsible_id
        task_text_template = settings.TASK_TEXT_NEW_WIN_EXISTING_LEAD
        logger.info(f"Сделка ID {lead_id} (или новая победа по ней) с ответственным ID {actual_lead_responsible_id}. Задача на него.")
    else: # Не удалось определить ответственного, ставим на Попову
        task_responsible_id = id_user_anastasia_popova
        task_text_template = "Проверить событие по сделке (ответственный не определен)"
        logger.warning(f"Ответственный для сделки ID {lead_id} не определен. Задача на '{settings.USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA}'.")
    
    if task_responsible_id:
        task_text = f"{task_text_template}: {purchase_winner_name} (Закупка: {purchase_data.purchase_number}, Сделка ID: {lead_id})"
        complete_till = int((datetime.now(timezone.utc) + timedelta(minutes=settings.TASK_COMPLETE_OFFSET_MINUTES)).timestamp())
        
        created_task = await amo_client.create_task_for_lead(
            lead_id, 
            task_responsible_id,
            task_text,
            complete_till,
            task_type_name=settings.TASK_TYPE_NAME_DEFAULT # Используем имя типа задачи из настроек
        )
        if created_task:
            logger.info(f"Задача для сделки ID {lead_id} (текст: '{text[:30]}...') успешно поставлена на пользователя ID {task_responsible_id}.")
        else:
            logger.error(f"Не удалось поставить задачу для сделки ID {lead_id} на пользователя ID {task_responsible_id}.")
    else:
        logger.warning(f"Не удалось определить ответственного для задачи по сделке ID {lead_id}.")


async def _handle_lead_processing(
    amo_client: AmoClient,
    purchase_data: DBStatePurchase,
    pipeline_id: int,
    target_status_id: int, # Этап, на который создаются новые сделки
    exclude_user_ids_for_creation_filter: List[int], # Менеджеры, для которых не создаем сделки
    id_user_anastasia_popova: Optional[int],
    id_user_unsorted: Optional[int]
):
    current_lead_id: Optional[int] = None
    is_new_lead = False
    lead_current_responsible_id: Optional[int] = None
    lead_current_budget: Optional[float] = 0.0

    if purchase_data.contract_securing is None or purchase_data.contract_securing < settings.MIN_LEAD_BUDGET:
        logger.debug(f"Пропуск (бюджет): '{purchase_data.winner_name}' ({purchase_data.purchase_number}), бюджет {purchase_data.contract_securing}")
        return

    deal_name = purchase_data.winner_name
    if not deal_name:
        logger.warning(f"Пропуск (нет имени победителя): закупка '{purchase_data.purchase_number}'")
        return
        
    logger.info(f"Обработка: '{deal_name}' (Закупка: {purchase_data.purchase_number}, ИНН: {purchase_data.inn})")

    # Фильтрация: не создавать сделки для победителей, закрепленных за Алена/Новикова Евгения
    if purchase_data.inn and exclude_user_ids_for_creation_filter:
        existing_companies = await amo_client.search_companies_by_inn(str(purchase_data.inn))
        for comp in existing_companies:
            if comp.get('responsible_user_id') in exclude_user_ids_for_creation_filter:
                logger.info(f"Победитель '{deal_name}' (ИНН: {purchase_data.inn}) через компанию ID {comp.get('id')} уже закреплен за исключенным менеджером. Новая сделка создаваться не будет.")
                return # Пропускаем эту запись из Excel

    # Поиск существующей сделки (по названию, в нужной воронке, исключая ответственных из settings.EXCLUDE_RESPONSIBLE_USERS)
    found_leads = await amo_client.search_leads_by_name(
        lead_name=deal_name,
        pipeline_id=pipeline_id,
        excluded_user_ids=exclude_user_ids_for_creation_filter 
    )

    if found_leads:
        lead_info = found_leads[0]
        current_lead_id = lead_info.get('id')
        lead_current_responsible_id = lead_info.get('responsible_user_id')
        lead_current_budget = float(lead_info.get('price', 0.0))
        logger.info(f"Найдена существующая сделка: '{deal_name}' (ID: {current_lead_id}, отв.: {lead_current_responsible_id})")

        # Обновление бюджета существующей сделки (ТЗ п. 6.3.3)
        new_budget_from_excel = purchase_data.contract_securing
        budget_changed_for_update = False
        if new_budget_from_excel is not None:
            new_budget_float = float(new_budget_from_excel)
            payload_update_lead: Dict[str, Any] = {}
            if new_budget_float != 0.0:
                if new_budget_float != lead_current_budget:
                    payload_update_lead["price"] = int(new_budget_float)
            if payload_update_lead:
                budget_changed_for_update = True
                payload_update_lead["updated_at"] = int(datetime.now(timezone.utc).timestamp())
                await amo_client.update_lead(current_lead_id, payload_update_lead)
                logger.info(f"Бюджет сделки ID {current_lead_id} обновлен на {new_budget_float}. Сделка перемещена вверх.")
    else:
        is_new_lead = True
        logger.info(f"Сделка для '{deal_name}' не найдена. Создание новой.")
        
        custom_fields_to_set: Dict[str, Any] = {} # {field_name_from_settings: value}
        if purchase_data.inn:
            custom_fields_to_set[settings.CUSTOM_FIELD_NAME_INN_LEAD] = str(purchase_data.inn)
        
        purchase_link_value = ""
        if purchase_data.purchase_number and purchase_data.eis_url: purchase_link_value = f"{purchase_data.purchase_number} {purchase_data.eis_url}"
        elif purchase_data.eis_url: purchase_link_value = purchase_data.eis_url
        elif purchase_data.purchase_number: purchase_link_value = f"Номер закупки: {purchase_data.purchase_number}"
        
        if purchase_link_value:
            custom_fields_to_set[settings.CUSTOM_FIELD_NAME_PURCHASE_LINK_LEAD] = purchase_link_value

        price_for_amo = float(purchase_data.contract_securing) if purchase_data.contract_securing is not None else 0.0
        
        # При создании новой сделки ответственный явно не указывается,
        # чтобы он мог быть назначен по умолчанию (возможно, "НЕРАЗОБРАННЫЕ ЗАЯВКИ")
        # Логика привязки/создания компании инкапсулирована в AmoClient.create_lead через company_inn
        responsible_user_id = settings.USER_NAME_UNSORTED_LEADS

        created_lead = await amo_client.create_lead(
            name=deal_name,
            price=price_for_amo,
            pipeline_id=pipeline_id,
            status_id=target_status_id,
            company_inn=str(purchase_data.inn) if purchase_data.inn else None,
            custom_fields_to_set=custom_fields_to_set if custom_fields_to_set else None,
            responsible_user_id=responsible_user_id
        )

        if created_lead:
            current_lead_id = created_lead.get('id')
            lead_current_responsible_id = created_lead.get('responsible_user_id') # Ответственный новой сделки
            lead_current_budget = price_for_amo
            logger.info(f"Сделка '{deal_name}' (ID: {current_lead_id}) успешно создана. Ответственный ID: {lead_current_responsible_id}")
        else:
            logger.error(f"Не удалось создать сделку '{deal_name}'.")
            return

    # Добавляем примечание и ставим задачу, если сделка есть (новая или существующая)
    if current_lead_id:
        note_text = generate_note_text_for_win(purchase_data)
        added_note = await amo_client.add_note_to_lead(current_lead_id, note_text)
        
        if added_note:
            logger.info(f"Примечание о победе ({purchase_data.purchase_number}) добавлено к сделке ID {current_lead_id}.")
            # Перемещение сделки вверх (п. 6.3.5) - если бюджет не обновлялся (иначе updated_at уже свежий)
            if not locals().get('budget_changed_for_update', False) and not is_new_lead: # Только для существующих, если бюджет не менялся
                 await amo_client.update_lead(current_lead_id, {"updated_at": int(datetime.now(timezone.utc).timestamp())})
                 logger.info(f"Сделка ID {current_lead_id} перемещена вверх из-за нового примечания.")
        
        await _create_task(
            amo_client, current_lead_id, lead_current_responsible_id,
            is_new_lead, deal_name,
            id_user_anastasia_popova, id_user_unsorted,
            purchase_data
        )


async def process_parsed_data_for_amocrm(
    amo_client: AmoClient,
    parsed_purchases: List[DBStatePurchase]
):
    logger.info(f"--- Запуск обработки {len(parsed_purchases)} записей для amoCRM ---")
    try:
        await amo_client._ensure_ids_initialized()
    except RuntimeError:
        logger.critical("Сбой инициализации ID amoCRM. Обработка остановлена.")
        return

    exclude_user_ids = []
    if settings.EXCLUDE_RESPONSIBLE_USERS:
        for user_name in settings.EXCLUDE_RESPONSIBLE_USERS:
            user_id = await amo_client.get_user_id(user_name)
            if user_id: exclude_user_ids.append(user_id)
            else: logger.warning(f"Пользователь '{user_name}' для исключения не найден.")
    
    pipeline_id = await amo_client.get_pipeline_id(settings.PIPELINE_NAME_GOSZAKAZ)
    if not pipeline_id: logger.error(f"Воронка '{settings.PIPELINE_NAME_GOSZAKAZ}' не найдена."); return

    target_status_id = await amo_client.get_status_id(pipeline_id, settings.STATUS_NAME_POBEDITELI)
    if not target_status_id: logger.error(f"Этап '{settings.STATUS_NAME_POBEDITELI}' не найден."); return
    
    id_anastasia_popova = await amo_client.get_user_id(settings.USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA)
    id_unsorted = await amo_client.get_user_id(settings.USER_NAME_UNSORTED_LEADS)
    if not id_anastasia_popova: logger.warning(f"ID пользователя '{settings.USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA}' не найден.")
    if not id_unsorted: logger.warning(f"ID пользователя '{settings.USER_NAME_UNSORTED_LEADS}' не найден.")

    for purchase_data in parsed_purchases:
        try:
            await _handle_lead_processing(
                amo_client, purchase_data, pipeline_id, target_status_id,
                exclude_user_ids, id_anastasia_popova, id_unsorted
            )
        except Exception as e:
            logger.error(f"Ошибка при обработке записи '{getattr(purchase_data, 'purchase_number', 'N/A')}': {e}", exc_info=True)
            
    logger.info(f"--- Обработка для amoCRM {len(parsed_purchases)} записей завершена ---")