# src/processing/amocrm_processor.py
import logging
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timezone # Добавлен timezone
import hashlib # Для генерации отпечатка данных

from src.amo.client import AmoClient
from src.amo.schemas import DBStatePurchase
from src.settings import settings

logger = logging.getLogger(__name__)

# Префикс для идентификации наших примечаний о победах
WIN_NOTE_ID_PREFIX = "WIN_ID:"

def format_value(value: Any, for_hash: bool = False) -> str:
    """Вспомогательная функция для форматирования значений."""
    if value is None:
        return "не указано" if not for_hash else "" # Для хеша пустая строка лучше
    if isinstance(value, date): # datetime является подклассом date
        return value.strftime('%d.%m.%Y') # Для примечания
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        else:
            return f"{value:.2f}" # Округляем до 2 знаков
    return str(value)

def generate_note_text_and_win_id(purchase_data: DBStatePurchase) -> tuple[str, str]:
    """
    Генерирует текст примечания и уникальный идентификатор победы.
    Идентификатор победы используется для проверки на дублирование.
    """
    # Идентификатор победы (например, по номеру закупки и дате итогов, если номер не уникален сам по себе)
    # Для ТЗ, где "главное поле/сущность – это Организация", а примечания - это "победы",
    # уникальность победы важна. "Номер закупки" (столбец A) должен быть ключом.
    win_id_key = format_value(purchase_data.purchase_number, for_hash=True) # Номер закупки как ключ победы

    note_lines = []
    # Добавляем ID победы в начало примечания (скрыто или явно)
    # Это поможет нам находить и сравнивать примечания для КОНКРЕТНОЙ победы.
    note_lines.append(f"{WIN_NOTE_ID_PREFIX}{win_id_key}") 
    
    # Формируем строки для примечания согласно ТЗ (п. 6.3.4)
    note_lines.append(f"Ссылка на закупку: {format_value(purchase_data.eis_url)}")
    note_lines.append(f"Наименование победителя: {format_value(purchase_data.winner_name)}")
    note_lines.append(f"ИНН: {format_value(purchase_data.inn)}")
    note_lines.append(f"Дата итогов: {format_value(purchase_data.result_date)}")
    note_lines.append(f"Наименование заказчика: {format_value(purchase_data.customer_name)}")
    note_lines.append(f"НМЦК: {format_value(purchase_data.nmck)}")
    note_lines.append(f"Обеспечение контракта: {format_value(purchase_data.contract_securing)}")
    note_lines.append(f"Обеспечение гарантийных обязательств: {format_value(purchase_data.warranty_obligations_securing)}")
    note_lines.append(f"Окончание контракта: {format_value(purchase_data.contract_end_date)}")
    note_lines.append(f"Цена победителя: {format_value(purchase_data.winner_price)}")
    
    contact_details_lines = []
    if purchase_data.fio_1 or purchase_data.phone_1 or purchase_data.email_1:
        contact_details_lines.append(
            f"  - Контакт 1: {format_value(purchase_data.fio_1)} / {format_value(purchase_data.phone_1)} / {format_value(purchase_data.email_1)}"
        )
    # ... (аналогично для Контакт 2 и Контакт 3) ...
    if purchase_data.fio_2 or purchase_data.phone_2 or purchase_data.email_2:
         contact_details_lines.append(
            f"  - Контакт 2: {format_value(purchase_data.fio_2)} / {format_value(purchase_data.phone_2)} / {format_value(purchase_data.email_2)}"
        )
    if purchase_data.fio_3 or purchase_data.phone_3 or purchase_data.email_3:
        contact_details_lines.append(
            f"  - Контакт 3: {format_value(purchase_data.fio_3)} / {format_value(purchase_data.phone_3)} / {format_value(purchase_data.email_3)}"
        )

    if contact_details_lines:
        note_lines.append("Контактные данные:")
        note_lines.extend(contact_details_lines)
    else:
        note_lines.append("Контактные данные: не указаны")

    note_lines.append(f"Преимущества СМП: {format_value(purchase_data.smp_advantages)}")
    note_lines.append(f"Статус СМП: {format_value(purchase_data.smp_status)}")

    return "\n".join(note_lines), win_id_key

async def _handle_lead_notes_and_updates(
    amo_client: AmoClient,
    lead_id: int,
    purchase_data: DBStatePurchase,
    lead_current_budget: Optional[float] # Текущий бюджет сделки из amoCRM
):
    """
    Обрабатывает примечания и обновления для существующей или новой сделки.
    - Обновляет бюджет сделки согласно ТЗ (п. 6.3.3).
    - Добавляет примечание о победе, если оно новое или данные изменились (п. 6.3.4, п. 6.3.5).
    - Перемещает сделку вверх списка при добавлении нового примечания (п. 6.3.5).
    """
    note_added_or_updated = False

    # 1. Обновление бюджета сделки (ТЗ п. 6.3.3)
    new_budget_from_excel = purchase_data.contract_securing
    if new_budget_from_excel is not None: # Если в Excel есть значение бюджета
        new_budget_float = float(new_budget_from_excel)
        update_payload = {}
        if new_budget_float != 0:
            if lead_current_budget is None or new_budget_float != float(lead_current_budget):
                update_payload["price"] = int(new_budget_float) # amoCRM ожидает целое для цены
                logger.info(f"Бюджет сделки ID {lead_id} будет обновлен на {new_budget_float} (старый: {lead_current_budget}).")
        else: # new_budget_float == 0, сохраняем старое значение (т.е. не обновляем)
            logger.info(f"Новый бюджет из Excel для сделки ID {lead_id} равен 0. Текущий бюджет ({lead_current_budget}) не будет изменен.")
        
        if update_payload: # Если есть что обновлять (кроме updated_at)
            # Для перемещения сделки вверх, обновляем updated_at
            update_payload["updated_at"] = int(datetime.now(timezone.utc).timestamp())
            updated_lead = await amo_client.update_lead(lead_id, update_payload)
            if updated_lead:
                logger.info(f"Бюджет и/или updated_at сделки ID {lead_id} успешно обновлены.")
            else:
                logger.warning(f"Не удалось обновить бюджет/updated_at для сделки ID {lead_id}.")


    # 2. Обработка примечаний
    new_note_full_text, win_id_key = generate_note_text_and_win_id(purchase_data)
    new_note_content_for_comparison = new_note_full_text.split("\n", 1)[1] # Текст после WIN_ID строки

    existing_notes = await amo_client.get_lead_notes(lead_id, note_types=["common"]) # Получаем только "common"
    
    found_matching_note = False
    if existing_notes:
        for note in existing_notes:
            note_params = note.get("params", {})
            note_text_existing = note_params.get("text", "") if note_params else "" # Убедимся что note_params существует
            
            if note_text_existing.startswith(f"{WIN_NOTE_ID_PREFIX}{win_id_key}"):
                existing_note_content = note_text_existing.split("\n", 1)[1]
                if existing_note_content == new_note_content_for_comparison:
                    logger.info(f"Примечание для победы ID '{win_id_key}' в сделке ID {lead_id} уже существует и данные совпадают. Новое примечание не добавляется.")
                    found_matching_note = True
                    break
                else:
                    logger.info(f"Примечание для победы ID '{win_id_key}' в сделке ID {lead_id} существует, но данные отличаются. Будет добавлено новое актуальное примечание.")
                    # Старое примечание с этим WIN_ID остается, добавляем новое актуальное.
                    # Это соответствует ТЗ "При появлении новых (последующих) побед ... добавляется примечание"
                    # Если "новые данные" означают обновление *существующей* победы, то нужно было бы удалять старое или редактировать.
                    # Но ТЗ говорит "добавляется примечание", что подразумевает новое.
                    # Если это ОДНА И ТА ЖЕ победа, но данные по ней изменились (например, цена победителя),
                    # то текущая логика добавит новое примечание с тем же WIN_ID, но обновленным текстом.
                    # Чтобы этого избежать, можно было бы сравнивать хэши от всего содержимого.
                    # Но для "последующих побед" - это добавление.
                    # Сейчас мы просто проверяем, есть ли идентичное. Если нет - добавляем.
                    found_matching_note = False # Считаем, что совпадения нет, т.к. текст отличается
                    break # Нашли примечание для этой победы, выходим из цикла по заметкам

    if not found_matching_note:
        added_note = await amo_client.add_note_to_lead(lead_id, new_note_full_text)
        if added_note:
            note_added_or_updated = True
            logger.info(f"Новое/обновленное примечание для победы ID '{win_id_key}' успешно добавлено к сделке ID {lead_id}.")
            # Перемещение сделки вверх списка при добавлении нового примечания (п. 6.3.5 ТЗ)
            # Обновляем поле updated_at сделки, чтобы она поднялась в списке
            # (если не обновляли бюджет, где updated_at уже был установлен)
            if not ("price" in locals().get("update_payload", {})): # Если бюджет не обновлялся в этом вызове
                timestamp_now = int(datetime.now(timezone.utc).timestamp())
                await amo_client.update_lead(lead_id, {"updated_at": timestamp_now})
                logger.info(f"Сделка ID {lead_id} перемещена вверх списка (updated_at: {timestamp_now}).")
        else:
            logger.warning(f"Не удалось добавить новое/обновленное примечание для победы ID '{win_id_key}' к сделке ID {lead_id}.")
    
    # TODO: Постановка задачи (п. 6.3.5, 6.3.6)
    # if note_added_or_updated or is_new_lead:
    #    ... логика определения ответственного для задачи и постановка ...
    #    responsible_for_task_id = ...
    #    task_text = f"Новая победа/обновление по сделке {lead_id} (Победитель: {purchase_data.winner_name})"
    #    complete_till = int((datetime.now(timezone.utc) + timedelta(minutes=10)).timestamp()) # Пример: +10 минут
    #    await amo_client.create_task_for_lead(lead_id, responsible_for_task_id, task_text, complete_till)


async def process_parsed_data_for_amocrm(
    amo_client: AmoClient,
    parsed_purchases: List[DBStatePurchase]
):
    logger.info(f"--- Запуск обработки {len(parsed_purchases)} записей для amoCRM ---")
    # ... (блок получения ID остается таким же) ...
    exclude_user_ids = []
    if settings.EXCLUDE_RESPONSIBLE_USERS:
        for user_name in settings.EXCLUDE_RESPONSIBLE_USERS:
            user_id = await amo_client.get_user_id(user_name)
            if user_id:
                exclude_user_ids.append(user_id)
                logger.info(f"Пользователь '{user_name}' (ID: {user_id}) будет исключен при поиске сделок.")
            else:
                logger.warning(f"Пользователь '{user_name}' для исключения не найден в amoCRM.")
    else:
        logger.info("Список исключаемых пользователей (EXCLUDE_RESPONSIBLE_USERS) не задан в настройках.")

    pipeline_id = await amo_client.get_pipeline_id(settings.PIPELINE_NAME_GOSZAKAZ)
    if not pipeline_id:
        logger.error(f"Воронка '{settings.PIPELINE_NAME_GOSZAKAZ}' не найдена. Обработка для amoCRM прервана.")
        return

    target_status_id = await amo_client.get_status_id(pipeline_id, settings.STATUS_NAME_POBEDITELI)
    if not target_status_id:
        logger.error(f"Целевой этап '{settings.STATUS_NAME_POBEDITELI}' в воронке '{settings.PIPELINE_NAME_GOSZAKAZ}' (ID: {pipeline_id}) не найден. Обработка для amoCRM прервана.")
        return
    logger.info(f"Новые сделки будут создаваться в воронке: '{settings.PIPELINE_NAME_GOSZAKAZ}' (ID: {pipeline_id}), на этапе: '{settings.STATUS_NAME_POBEDITELI}' (ID: {target_status_id})")

    inn_field_name = settings.CUSTOM_FIELD_NAME_INN_LEAD # Имя поля из настроек
    purchase_link_field_name = settings.CUSTOM_FIELD_NAME_PURCHASE_LINK_LEAD # Имя поля из настроек

    for purchase_data in parsed_purchases:
        current_lead_id: Optional[int] = None
        is_new_lead = False
        lead_current_budget: Optional[float] = None

        try:
            if purchase_data.contract_securing is None or purchase_data.contract_securing < settings.MIN_LEAD_BUDGET: # Строго БОЛЬШЕ в ТЗ, но код был <=. Исправлено на < settings.MIN_LEAD_BUDGET
                logger.debug(f"Пропуск '{purchase_data.purchase_number}' (Победитель: {purchase_data.winner_name}): Бюджет ({purchase_data.contract_securing}) меньше {settings.MIN_LEAD_BUDGET}.")
                continue
            
            company_inn = str(purchase_data.inn)

            deal_name = purchase_data.winner_name
            if not deal_name:
                logger.warning(f"Пропуск '{purchase_data.purchase_number}': Отсутствует имя победителя.")
                continue

            logger.info(f"Обработка: {deal_name} (Закупка: {purchase_data.purchase_number})")
            
            found_leads = await amo_client.search_leads_by_name(
                lead_name=deal_name,
                pipeline_id=pipeline_id,
                excluded_user_ids=exclude_user_ids
            )

            if found_leads:
                lead_info = found_leads[0]
                current_lead_id = lead_info.get('id')
                lead_current_budget = float(lead_info.get('price', 0)) if lead_info.get('price') is not None else 0.0
                logger.info(f"Найдена существующая подходящая сделка для '{deal_name}' (ID: {current_lead_id}, текущий бюджет: {lead_current_budget}).")
                
                await amo_client.create_company(
                    name=deal_name,
                    inn=company_inn
                )
            else:
                is_new_lead = True
                logger.info(f"Подходящая сделка для '{deal_name}' не найдена. Создание новой.")
                lead_custom_fields_payload: Dict[str, Any] = {}

                if purchase_data.inn:
                    lead_custom_fields_payload[inn_field_name] = str(purchase_data.inn)

                purchase_link_value = ""
                if purchase_data.purchase_number and purchase_data.eis_url:
                    purchase_link_value = f"{purchase_data.purchase_number} {purchase_data.eis_url}"
                elif purchase_data.eis_url:
                    purchase_link_value = purchase_data.eis_url
                elif purchase_data.purchase_number:
                    purchase_link_value = f"Номер закупки: {purchase_data.purchase_number}"
                
                if purchase_link_value: # Проверяем, что ссылка сформирована
                    lead_custom_fields_payload[purchase_link_field_name] = purchase_link_value

                price_for_amo = 0.0
                if purchase_data.contract_securing is not None:
                    price_for_amo = float(purchase_data.contract_securing)
                
                # TODO: Логика назначения ответственного (п. 6.3.6)
                # responsible_user_id_for_new_lead = ...


                created_lead = await amo_client.create_lead(
                    name=deal_name,
                    price=price_for_amo,
                    pipeline_id=pipeline_id,
                    status_id=target_status_id,
                    company_inn=company_inn,
                    custom_fields=lead_custom_fields_payload if lead_custom_fields_payload else None,
                    # responsible_user_id=responsible_user_id_for_new_lead
                )

                if created_lead:
                    current_lead_id = created_lead.get('id')
                    lead_current_budget = price_for_amo # Бюджет только что созданной сделки
                    logger.info(f"Сделка '{deal_name}' (ID: {current_lead_id}) успешно создана с бюджетом {price_for_amo}.")
                else:
                    logger.error(f"Не удалось создать сделку '{deal_name}'. Пропуск дальнейшей обработки этой записи.")
                    continue

            if current_lead_id:
                await _handle_lead_notes_and_updates(
                    amo_client,
                    current_lead_id,
                    purchase_data,
                    lead_current_budget
                )

        except Exception as e:
            logger.error(f"Критическая ошибка при обработке закупки '{getattr(purchase_data, 'purchase_number', 'N/A')}' для amoCRM: {e}", exc_info=True)
            
    logger.info(f"--- Обработка для amoCRM {len(parsed_purchases)} записей завершена ---")