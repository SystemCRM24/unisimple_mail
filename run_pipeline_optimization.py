from src.settings import settings
from src.amo.client import AmoClient
import src.services.amo_pipeline_services as amo_services
import asyncio
import logging

# Настраиваем базовое логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def run_pipeline_optimization_tests():
    """
    Основная функция для запуска тестов оптимизации воронки на тестовом аккаунте.
    """
    if not settings.is_test_mode:
        logger.error("Скрипт запущен НЕ в тестовом режиме (MODE != 'test' в .env). Запуск операций на боевом аккаунте опасен! Прерывание.")
        print("\n!!! ОШИБКА: Скрипт должен быть запущен в тестовом режиме. ")
        print("Убедитесь, что в вашем файле .env строка 'MODE=\"test\"' или 'MODE=test' (без кавычек) присутствует и корректна.")
        return

    logger.info("Запуск тестов оптимизации воронки в тестовом режиме...")

    logger.info(f"Используется поддомен: {settings.current_amo_subdomain}")

    async with AmoClient() as amo_client:
        # 1. Получаем ID сущностей из тестового аккаунта
        logger.info("Получение ID сущностей из тестового аккаунта...")
        # Передаем клиент в функцию, которая обновит глобальные переменные в amo_services
        await amo_services.get_amo_entity_ids(amo_client)

        # Выводим полученные ID, обращаясь к ним через имя модуля
        print("\n--- Полученные ID сущностей (проверьте их в своей песочнице!) ---")
        print(f"Воронка '{amo_services.PIPELINE_NAME_GOV_ORDER}': {amo_services.PIPELINE_ID_GOV_ORDER}")
        print(f"Этап '{amo_services.STAGE_NAME_WINNERS}': {amo_services.STAGE_ID_WINNERS}")
        print(f"Этап '{amo_services.STAGE_NAME_ACCREDITATION}': {amo_services.STAGE_ID_ACCREDITATION}")
        print(f"Этап '{amo_services.STAGE_NAME_PARTICIPANTS}': {amo_services.STAGE_ID_PARTICIPANTS}")
        print(f"Этап '{amo_services.STAGE_NAME_COLD_LEADS}': {amo_services.STAGE_ID_COLD_LEADS}")
        print(f"Этап '{amo_services.STAGE_NAME_PRIMARY_NEGOTIATIONS}': {amo_services.STAGE_ID_PRIMARY_NEGOTIATIONS}")
        print(f"Этап '{amo_services.STAGE_NAME_LPR_NEGOTIATIONS}': {amo_services.STAGE_ID_LPR_NEGOTIATIONS}")
        print(f"Этап '{amo_services.STAGE_NAME_NEGOTIATIONS}': {amo_services.STAGE_ID_NEGOTIATIONS} (None, если этапа еще нет)")
        print(f"Пользователь '{amo_services.RESPONSIBLE_USER_NAME_UNASSIGNED}': {amo_services.USER_ID_UNASSIGNED}")
        print(f"Пользователь '{amo_services.RESPONSIBLE_USER_NAME_ANASTASIA}': {amo_services.USER_ID_ANASTASIA}")
        print(f"Пользователь '{amo_services.RESPONSIBLE_USER_NAME_ILYA}': {amo_services.USER_ID_ILYA}")
        print(f"Пользователь '{amo_services.RESPONSIBLE_USER_NAME_RABOTNIK}': {amo_services.USER_ID_RABOTNIK}")
        # Убедитесь, что имена переменных пользователей соответствуют вашим глобальным переменным в сервисном файле
        print("-------------------------------------------------------------\n")


        # --- Выполнение задач по оптимизации ---
        # Теперь обращаемся к ID через имя модуля
        # 2. Удаление сделок 'Победители' с ответственным 'НЕРАЗОБРАННЫЕ ЗАЯВКИ'
        if amo_services.PIPELINE_ID_GOV_ORDER and amo_services.STAGE_ID_WINNERS is not None and amo_services.USER_ID_UNASSIGNED is not None:
             await amo_services.delete_winner_deals_unassigned(amo_client) # Вызываем функцию из модуля
        else:
            logger.warning("Пропуск задачи удаления сделок 'Победители': не все необходимые ID получены.")


        # 3. Оптимизация этапов 'Аккредитация' и 'Участники'
        if amo_services.PIPELINE_ID_GOV_ORDER and amo_services.STAGE_ID_ACCREDITATION is not None and amo_services.STAGE_ID_PARTICIPANTS is not None and amo_services.STAGE_ID_COLD_LEADS is not None and amo_services.USER_ID_UNASSIGNED is not None:
             await amo_services.optimize_accreditation_and_participants_stages(amo_client) # Вызываем функцию из модуля
             # После выполнения этой задачи, нужно снова получить ID этапов, чтобы видеть актуальное состояние
             logger.info("Повторное получение ID сущностей после оптимизации этапов (для проверки)...")
             await amo_services.get_amo_entity_ids(amo_client) # Перезаполняем глобальные ID в модуле amo_services
             print("\n--- Полученные ID сущностей после optimize_accreditation_and_participants_stages ---")
             print(f"Этап '{amo_services.STAGE_NAME_ACCREDITATION}': {amo_services.STAGE_ID_ACCREDITATION}") # Ожидается None
             print(f"Этап '{amo_services.STAGE_NAME_PARTICIPANTS}': {amo_services.STAGE_ID_PARTICIPANTS}") # Ожидается None
             print(f"Этап '{amo_services.STAGE_NAME_COLD_LEADS}': {amo_services.STAGE_ID_COLD_LEADS}") # Должен быть
             print("-------------------------------------------------------------\n")

        else:
             logger.warning("Пропуск задачи оптимизации этапов 'Аккредитация' и 'Участники': не все необходимые ID получены ПЕРЕД выполнением этой задачи.")


        # 4. Объединение этапов переговоров
        # if amo_services.PIPELINE_ID_GOV_ORDER and amo_services.STAGE_ID_PRIMARY_NEGOTIATIONS is not None and amo_services.STAGE_ID_LPR_NEGOTIATIONS is not None:
        #      await amo_services.merge_negotiation_stages(amo_client) # Вызываем функцию из модуля
        #      # После выполнения этой задачи, нужно снова получить ID этапов
        #      logger.info("Повторное получение ID сущностей после объединения этапов переговоров (для проверки)...")
        #      await amo_services.get_amo_entity_ids(amo_client) # Перезаполняем глобальные ID в модуле amo_services
        #      print("\n--- Полученные ID сущностей после merge_negotiation_stages ---")
        #      print(f"Этап '{amo_services.STAGE_NAME_PRIMARY_NEGOTIATIONS}': {amo_services.STAGE_ID_PRIMARY_NEGOTIATIONS}") # Ожидается None
        #      print(f"Этап '{amo_services.STAGE_NAME_LPR_NEGOTIATIONS}': {amo_services.STAGE_ID_LPR_NEGOTIATIONS}") # Ожидается None
        #      print(f"Этап '{amo_services.STAGE_NAME_NEGOTIATIONS}': {amo_services.STAGE_ID_NEGOTIATIONS}") # Должен быть ID
        #      print("-------------------------------------------------------------\n")
        # else:
        #      logger.warning("Пропуск задачи объединения этапов переговоров: не все необходимые ID получены ПЕРЕД выполнением этой задачи.")

        # TODO: Добавь вызовы других реализованных сервисных функций здесь по мере их готовности

    logger.info("Тесты оптимизации воронки завершены.")


if __name__ == '__main__':
    # Запуск асинхронной функции
    # Переменная окружения MODE="test" должна быть установлена перед запуском скрипта
    asyncio.run(run_pipeline_optimization_tests())