import asyncio
import logging
import time

# Импортируйте константы из client.py
from src.amo.client import AmoClient, PIPELINE_NAME_GOSZAKAZ, STATUS_NAME_POBEDITELI, CUSTOM_FIELD_NAME_INN

# Настройка логирования (убедитесь, что это настроено правильно, например, на DEBUG)
logging.basicConfig(
    level=logging.INFO, # Измените на logging.DEBUG, чтобы увидеть больше отладочной информации
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def run_client_tests():
    logger.info("\n--- Запуск всех тестов amoCRM ---")
    async with AmoClient() as amo_client:
        test_company_id = None

        try:
            logger.info("\n--- Тест: Создание компании ---")
            test_company_name = f"Тестовая компания {int(time.time())}"
            test_inn = f"TEST{int(time.time())}"

            # Поиск по ИНН
            inn_field_id = await amo_client.get_company_custom_field_id(CUSTOM_FIELD_NAME_INN)
            if inn_field_id:
                found_companies_by_inn = await amo_client.search_companies_by_inn(test_inn)
                if found_companies_by_inn:
                    logger.info(f"Компании по ИНН '{test_inn}' найдены (ожидаемое поведение).")
                    test_company_id = found_companies_by_inn[0]['id']
                else:
                    logger.info(f"Компании по ИНН '{test_inn}' не найдены (ожидаемое поведение).")
            else:
                logger.warning(f"Пользовательское поле '{CUSTOM_FIELD_NAME_INN}' (ИНН) не найдено. Поиск по ИНН невозможен.")
            
            if test_company_id is None:
                logger.info(f"Попытка создания компании '{test_company_name}' (ИНН: {test_inn})...")
                created_company = await amo_client.create_company(name=test_company_name, inn=test_inn)
                if created_company:
                    test_company_id = created_company['id']
                    logger.info(f"Компания '{test_company_name}' успешно создана. ID: {test_company_id}")
                else:
                    # Убрано дублирующее сообщение об ошибке, т.к. client.py уже логирует причину
                    logger.error(f"Не удалось создать компанию '{test_company_name}'.") 
                    test_company_id = None
            else:
                logger.info(f"Используется существующая компания ID: {test_company_id}")

        except Exception as e:
            logger.error(f"Произошла необработанная ошибка при тесте создания компании: {e}", exc_info=True)

        # Проверка, что компания успешно создана перед созданием сделки
        if test_company_id:
            logger.info("\n--- Тест: Создание сделки ---")
            
            # Получаем ID воронки и этапа динамически
            pipeline_id_for_test = await amo_client.get_pipeline_id(PIPELINE_NAME_GOSZAKAZ)
            status_id_for_test = await amo_client.get_status_id(pipeline_id_for_test, STATUS_NAME_POBEDITELI)

            if pipeline_id_for_test is not None and status_id_for_test is not None:
                test_new_deal_name = f"Тестовая сделка создания {int(time.time())}"
                # Логирование с использованием полученных ID
                logger.info(f"Попытка создания сделки '{test_new_deal_name}' в воронке ID {pipeline_id_for_test}, этап ID {status_id_for_test}...")
                
                created_lead = await amo_client.create_lead(
                    name=test_new_deal_name,
                    price=100.0,
                    pipeline_id=pipeline_id_for_test,
                    status_id=status_id_for_test,
                    company_id=test_company_id # Привязываем к созданной тестовой компании (если успешно создана)
                )

                if created_lead:
                    logger.info(f"Сделка '{test_new_deal_name}' успешно создана. ID: {created_lead['id']}")
                    # Тест: Поиск созданной сделки по имени
                    found_leads = await amo_client.search_leads_by_name(test_new_deal_name, pipeline_id_for_test)
                    if found_leads:
                        logger.info(f"Найдена созданная сделка по имени: {found_leads[0]['name']}")
                        # Упрощенная проверка привязки компании
                        if test_company_id: # Проверяем, что была попытка привязать компанию
                            logger.info(f"Предполагается, что компания ID {test_company_id} привязана к сделке.")
                        else:
                            logger.info("Сделка создана без привязки компании.")
                    else:
                        logger.error(f"Созданная сделка '{test_new_deal_name}' не найдена по имени.")
                else:
                    logger.error(f"Не удалось создать сделку '{test_new_deal_name}'.")
            else:
                logger.error("Не удалось получить необходимые ID воронки или этапа для создания сделки. Проверьте настройки amoCRM и константы в client.py.")

        else:
            logger.error("Пропуск теста создания сделки, так как не удалось создать компанию.")
            
        # Тест: Поиск несуществующей компании по ИНН
        logger.info("\n--- Тест: Поиск несуществующей компании ---")
        non_existent_inn = "9999999999"
        logger.info(f"Поиск компаний по ИНН '{non_existent_inn}'...")
        inn_field_id = await amo_client.get_company_custom_field_id(CUSTOM_FIELD_NAME_INN)
        if inn_field_id:
            found_non_existent = await amo_client.search_companies_by_inn(non_existent_inn)
            if not found_non_existent:
                logger.info(f"Компании по ИНН '{non_existent_inn}' не найдены (ожидаемое поведение).")
            else:
                logger.error(f"Найдена несуществующая компания по ИНН '{non_existent_inn}'.")
        else:
            logger.warning(f"Пользовательское поле '{CUSTOM_FIELD_NAME_INN}' (ИНН) не найдено. Поиск по ИНН невозможен.")


    logger.info("\n--- Все тесты завершены ---")

if __name__ == "__main__":
    asyncio.run(run_client_tests())