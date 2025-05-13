import logging
from typing import List, Dict, Any

from src.amo.client import AmoClient
from src.amo.schemas import DBStatePurchase
from src.settings import settings

logger = logging.getLogger(__name__)


async def process_parsed_data_for_amocrm(
    amo_client: AmoClient,
    parsed_purchases: List[DBStatePurchase]
):
    logger.info(f"--- Запуск обработки {len(parsed_purchases)} записей для amoCRM ---")

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

    inn_field_id_lead = await amo_client.get_custom_field_id(settings.CUSTOM_FIELD_NAME_INN_LEAD)
    if not inn_field_id_lead:
        logger.warning(f"Пользовательское поле сделки '{settings.CUSTOM_FIELD_NAME_INN_LEAD}' (ИНН) не найдено.")

    purchase_link_field_id_lead = await amo_client.get_custom_field_id(settings.CUSTOM_FIELD_NAME_PURCHASE_LINK_LEAD)
    if not purchase_link_field_id_lead:
        logger.warning(f"Пользовательское поле сделки '{settings.CUSTOM_FIELD_NAME_PURCHASE_LINK_LEAD}' (Ссылка на закупку) не найдено.")

    for purchase_data in parsed_purchases:
        try:
            if purchase_data.contract_securing is None or purchase_data.contract_securing <= settings.MIN_LEAD_BUDGET:
                logger.debug(f"Пропуск '{purchase_data.purchase_number}' (Победитель: {purchase_data.winner_name}): Бюджет ({purchase_data.contract_securing}) не превышает {settings.MIN_LEAD_BUDGET}.")
                continue

            deal_name = purchase_data.winner_name
            if not deal_name:
                logger.warning(f"Пропуск '{purchase_data.purchase_number}': Отсутствует имя победителя.")
                continue

            logger.info(f"Поиск сделок: '{deal_name}' в воронке ID {pipeline_id}, исключая ответственных с ID: {exclude_user_ids}")
            found_leads = await amo_client.search_leads_by_name(
                lead_name=deal_name,
                pipeline_id=pipeline_id,
                excluded_user_ids=exclude_user_ids
            )

            if found_leads:
                logger.info(f"Найдена существующая подходящая сделка для '{deal_name}' (ID: {found_leads[0].get('id')}). Создание новой сделки пропускается.")
            else:
                logger.info(f"Подходящая сделка для '{deal_name}' не найдена. Создание новой сделки.")

                lead_custom_fields_payload: Dict[str, Any] = {}

                if inn_field_id_lead and purchase_data.inn:
                    lead_custom_fields_payload[settings.CUSTOM_FIELD_NAME_INN_LEAD] = str(purchase_data.inn)

                purchase_link_value = ""
                if purchase_data.purchase_number and purchase_data.eis_url:
                    purchase_link_value = f"{purchase_data.purchase_number} {purchase_data.eis_url}"
                elif purchase_data.eis_url:
                    purchase_link_value = purchase_data.eis_url
                elif purchase_data.purchase_number:
                    purchase_link_value = f"{purchase_data.purchase_number}"
                
                if purchase_link_field_id_lead and purchase_link_value:
                    lead_custom_fields_payload[settings.CUSTOM_FIELD_NAME_PURCHASE_LINK_LEAD] = purchase_link_value

                price_for_amo = 0
                if purchase_data.contract_securing is not None:
                    price_for_amo = int(purchase_data.contract_securing)
                else:
                    logger.warning(f"Для сделки '{deal_name}' отсутствует 'Обеспечение контракта'. Бюджет будет установлен в 0.")
                
                company_id_to_link = None

                created_lead = await amo_client.create_lead(
                    name=deal_name,
                    price=price_for_amo,
                    pipeline_id=pipeline_id,
                    status_id=target_status_id,
                    custom_fields=lead_custom_fields_payload if lead_custom_fields_payload else None,
                    company_id=company_id_to_link
                )

                if created_lead:
                    new_lead_id = created_lead.get('id')
                    logger.info(f"Сделка '{deal_name}' (ID: {new_lead_id}) успешно создана.")

                else:
                    logger.error(f"Не удалось создать сделку '{deal_name}'.")
        except Exception as e:
            logger.error(f"Критическая ошибка при обработке закупки '{getattr(purchase_data, 'purchase_number', 'N/A')}' для amoCRM: {e}", exc_info=True)
            
    logger.info(f"--- Обработка для amoCRM {len(parsed_purchases)} записей завершена ---")