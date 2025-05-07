import asyncio
import logging
from typing import Optional

# Исправлен импорт: AmoClient теперь находится в src.client
from src.amo.client import AmoClient
from src.settings import settings

logger = logging.getLogger(__name__)

# --- Глобальные переменные для хранения ID сущностей (для простоты примера) ---

PIPELINE_NAME_GOV_ORDER = "Гос.заказ - прогрев клиента"
STAGE_NAME_WINNERS = "Победители"
RESPONSIBLE_USER_NAME_UNASSIGNED = "НЕРАЗОБРАННЫЕ ЗАЯВКИ"
RESPONSIBLE_USER_NAME_ANASTASIA = "Анастасия Попова" # Оставляем, если нужен по ТЗ
STAGE_NAME_COLD_LEADS = "Холодные заявки"
STAGE_NAME_ACCREDITATION = "Аккредитация"
STAGE_NAME_PARTICIPANTS = "Участники"
STAGE_NAME_PRIMARY_NEGOTIATIONS = "Первичные переговоры"
STAGE_NAME_LPR_NEGOTIATIONS = "Переговоры с ЛПР"
STAGE_NAME_NEGOTIATIONS = "Переговоры" # Название нового объединенного этапа

# Добавляем имена новых пользователей, которые есть в твоем аккаунте
RESPONSIBLE_USER_NAME_ILYA = "Илья"
RESPONSIBLE_USER_NAME_RABOTNIK = "test@mail.ru"
# Оставляем имена "Алена" и "Новикова Евгения", если они все еще релевантны для других частей ТЗ,
# или удаляем, если Илья и Работник их полностью заменяют.
# Для ясности пока оставим, но будем искать Илью и Работника.
RESPONSIBLE_USER_NAME_ALENA = "Алена" # Возможно, больше не нужен
RESPONSIBLE_USER_NAME_NOVIKOVA = "Новикова Евгения" # Возможно, больше не нужен


PIPELINE_ID_GOV_ORDER: Optional[int] = None
STAGE_ID_WINNERS: Optional[int] = None
USER_ID_UNASSIGNED: Optional[int] = None
USER_ID_ANASTASIA: Optional[int] = None # ID Анастасии Поповой
STAGE_ID_COLD_LEADS: Optional[int] = None
STAGE_ID_ACCREDITATION: Optional[int] = None
STAGE_ID_PARTICIPANTS: Optional[int] = None
STAGE_ID_PRIMARY_NEGOTIATIONS: Optional[int] = None
STAGE_ID_LPR_NEGOTIATIONS: Optional[int] = None
STAGE_ID_NEGOTIATIONS: Optional[int] = None

# Добавляем глобальные переменные для ID новых пользователей
USER_ID_ILYA: Optional[int] = None
USER_ID_RABOTNIK: Optional[int] = None

USER_ID_ALENA: Optional[int] = None # Возможно, больше не нужен
USER_ID_NOVIKOVA: Optional[int] = None # Возможно, больше не нужен


async def get_amo_entity_ids(amo_client: AmoClient):
    """
    Получает необходимые ID воронок, этапов и пользователей из amoCRM
    и сохраняет их в глобальных переменных.
    """
    global PIPELINE_ID_GOV_ORDER, STAGE_ID_WINNERS, USER_ID_UNASSIGNED, \
        USER_ID_ANASTASIA, STAGE_ID_COLD_LEADS, STAGE_ID_ACCREDITATION, \
        STAGE_ID_PARTICIPANTS, STAGE_ID_PRIMARY_NEGOTIATIONS, \
        STAGE_ID_LPR_NEGOTIATIONS, STAGE_ID_NEGOTIATIONS, \
        USER_ID_ALENA, USER_ID_NOVIKOVA, USER_ID_ILYA, USER_ID_RABOTNIK # Добавили новые глобальные переменные


    logger.info("Получение ID сущностей из amoCRM...")
    pipelines = await amo_client.get_pipelines_with_statuses()
    if pipelines:
        for pipeline in pipelines:
            if pipeline.get('name') == PIPELINE_NAME_GOV_ORDER:
                PIPELINE_ID_GOV_ORDER = pipeline.get('id')
                logger.info(f"Найден ID воронки '{PIPELINE_NAME_GOV_ORDER}': {PIPELINE_ID_GOV_ORDER}")
                if '_embedded' in pipeline and 'statuses' in pipeline['_embedded']:
                    for status in pipeline['_embedded']['statuses']:
                        if status.get('name') == STAGE_NAME_WINNERS:
                            STAGE_ID_WINNERS = status.get('id')
                        elif status.get('name') == STAGE_NAME_COLD_LEADS:
                            STAGE_ID_COLD_LEADS = status.get('id')
                        elif status.get('name') == STAGE_NAME_ACCREDITATION:
                            STAGE_ID_ACCREDITATION = status.get('id')
                        elif status.get('name') == STAGE_NAME_PARTICIPANTS:
                            STAGE_ID_PARTICIPANTS = status.get('id')
                        elif status.get('name') == STAGE_NAME_PRIMARY_NEGOTIATIONS:
                            STAGE_ID_PRIMARY_NEGOTIATIONS = status.get('id')
                        elif status.get('name') == STAGE_NAME_LPR_NEGOTIATIONS:
                            STAGE_ID_LPR_NEGOTIATIONS = status.get('id')
                        elif status.get('name') == STAGE_NAME_NEGOTIATIONS:
                             STAGE_ID_NEGOTIATIONS = status.get('id')
                    logger.info(f"Получены ID этапов для воронки '{PIPELINE_NAME_GOV_ORDER}'.")


    users = await amo_client.get_users()
    if users:
        for user in users:
            if user.get('name') == RESPONSIBLE_USER_NAME_UNASSIGNED:
                USER_ID_UNASSIGNED = user.get('id')
            elif user.get('name') == RESPONSIBLE_USER_NAME_ANASTASIA:
                USER_ID_ANASTASIA = user.get('id')
            # Обновляем поиск пользователей на "Илья" и "Работник"
            elif user.get('name') == RESPONSIBLE_USER_NAME_ILYA:
                 USER_ID_ILYA = user.get('id')
            elif user.get('name') == RESPONSIBLE_USER_NAME_RABOTNIK:
                 USER_ID_RABOTNIK = user.get('id')
            # Оставляем поиск "Алены" и "Новиковой" на всякий случай, если их имена все еще могут встретиться,
            # но вероятно, их можно удалить, если они не используются по ТЗ.
            elif user.get('name') == RESPONSIBLE_USER_NAME_ALENA:
                 USER_ID_ALENA = user.get('id')
            elif user.get('name') == RESPONSIBLE_USER_NAME_NOVIKOVA:
                 USER_ID_NOVIKOVA = user.get('id')

        logger.info("Получены ID пользователей.")


    # Проверка, что все критически важные ID были получены
    # Определи, какие ID являются критически важными для твоих текущих задач.
    # Например, для оптимизации воронки нужны ID воронки, этапов для обработки/перемещения,
    # и ID ответственного "НЕРАЗОБРАННЫЕ ЗАЯВКИ".
    critical_ids = {
        'PIPELINE_ID_GOV_ORDER': PIPELINE_ID_GOV_ORDER,
        'USER_ID_UNASSIGNED': USER_ID_UNASSIGNED,
        'STAGE_ID_COLD_LEADS': STAGE_ID_COLD_LEADS, # Нужен для перемещения сделок
        # Добавь сюда ID этапов "Победители", "Аккредитация", "Участники", "Первичные переговоры", "Переговоры с ЛПР",
        # если их отсутствие сделает невозможным выполнение соответствующих задач.
        # Их ID могут отсутствовать, если этапы уже удалены предыдущими запусками скрипта,
        # но критично, если они нужны для текущего шага.
        # Например, для удаления сделок на этапе Победители, нужен STAGE_ID_WINNERS.
        # Для оптимизации Аккредитации/Участников, нужны STAGE_ID_ACCREDITATION и STAGE_ID_PARTICIPANTS.
        # Для объединения переговоров, нужны STAGE_ID_PRIMARY_NEGOTIATIONS и STAGE_ID_LPR_NEGOTIATIONS.
    }

    # Добавляем проверку наличия ID Ильи и Работника, если они нужны для других задач
    if RESPONSIBLE_USER_NAME_ILYA: # Проверяем, что имя пользователя определено
        critical_ids[f'USER_ID_{RESPONSIBLE_USER_NAME_ILYA}'] = USER_ID_ILYA
    if RESPONSIBLE_USER_NAME_RABOTNIK:
         critical_ids[f'USER_ID_{RESPONSIBLE_USER_NAME_RABOTNIK}'] = USER_ID_RABOTNIK

    missing_critical_ids = [name for name, id_val in critical_ids.items() if id_val is None]

    if missing_critical_ids:
        logger.error(f"ВНИМАНИЕ: Не удалось получить критически важные ID: {', '.join(missing_critical_ids)}. "
                     f"Некоторые операции могут быть не выполнены.")
    else:
         logger.info("Все основные ID сущностей успешно получены.")

    # Обновляем вывод для включения новых пользователей
    logger.info(f"Текущие ID сущностей: "
                f"Воронка '{PIPELINE_NAME_GOV_ORDER}': {PIPELINE_ID_GOV_ORDER}, "
                f"Этап '{STAGE_NAME_WINNERS}': {STAGE_ID_WINNERS}, "
                f"Этап '{STAGE_NAME_ACCREDITATION}': {STAGE_ID_ACCREDITATION}, "
                f"Этап '{STAGE_NAME_PARTICIPANTS}': {STAGE_ID_PARTICIPANTS}, "
                f"Этап '{STAGE_NAME_COLD_LEADS}': {STAGE_ID_COLD_LEADS}, "
                f"Этап '{STAGE_NAME_PRIMARY_NEGOTIATIONS}': {STAGE_ID_PRIMARY_NEGOTIATIONS}, "
                f"Этап '{STAGE_NAME_LPR_NEGOTIATIONS}': {STAGE_ID_LPR_NEGOTIATIONS}, "
                 f"Этап '{STAGE_NAME_NEGOTIATIONS}': {STAGE_ID_NEGOTIATIONS}, "
                f"Пользователь '{RESPONSIBLE_USER_NAME_UNASSIGNED}': {USER_ID_UNASSIGNED}, "
                f"Пользователь '{RESPONSIBLE_USER_NAME_ANASTASIA}': {USER_ID_ANASTASIA}, "
                f"Пользователь '{RESPONSIBLE_USER_NAME_ILYA}': {USER_ID_ILYA}, " # Выводим ID Ильи
                 f"Пользователь '{RESPONSIBLE_USER_NAME_RABOTNIK}': {USER_ID_RABOTNIK}.") # Выводим ID Работника
                # Можно также вывести ID Алены и Новиковой, если они остались в списке поиска
                # f"Пользователь '{RESPONSIBLE_USER_NAME_ALENA}': {USER_ID_ALENA}, "
                # f"Пользователь '{RESPONSIBLE_USER_NAME_NOVIKOVA}': {USER_ID_NOVIKOVA}.")


async def delete_winner_deals_unassigned(amo_client: AmoClient):
    """
    Удаляет сделки на этапе 'Победители' в воронке 'Гос.заказ - прогрев клиента'
    с ответственным 'НЕРАЗОБРАННЫЕ ЗАЯВКИ'.
    """
    logger.info("Начало выполнения задачи: Удаление сделок 'Победители' с ответственным 'НЕРАЗОБРАННЫЕ ЗАЯВКИ'.")
    # Эта задача зависит только от ID воронки, этапа "Победители" и пользователя "НЕРАЗОБРАННЫЕ ЗАЯВКИ"
    if not all([PIPELINE_ID_GOV_ORDER, STAGE_ID_WINNERS, USER_ID_UNASSIGNED]):
        logger.error("Не удалось выполнить удаление: не все необходимые ID сущностей известны.")
        return

    logger.info(f"Поиск сделок для удаления в воронке '{PIPELINE_NAME_GOV_ORDER}' ({PIPELINE_ID_GOV_ORDER}), "
                f"этапе '{STAGE_NAME_WINNERS}' ({STAGE_ID_WINNERS}), "
                f"ответственный '{RESPONSIBLE_USER_NAME_UNASSIGNED}' ({USER_ID_UNASSIGNED}).")

    leads_to_delete = await amo_client.get_leads(
        pipeline_id=PIPELINE_ID_GOV_ORDER,
        status_id=STAGE_ID_WINNERS,
        responsible_user_id=USER_ID_UNASSIGNED
    )

    if not leads_to_delete:
        logger.info("Сделок для удаления не найдено.")
        return

    logger.info(f"Найдено {len(leads_to_delete)} сделок для удаления.")

    for lead in leads_to_delete:
        lead_id = lead.get('id')
        lead_name = lead.get('name', 'Без названия')
        logger.info(f"Удаление сделки: ID={lead_id}, Название='{lead_name}'")
        success = await amo_client.delete_lead(lead_id)
        if success:
            logger.info(f"Сделка ID={lead_id} успешно удалена.")
        else:
            logger.error(f"Ошибка при удалении сделки ID={lead_id}.")
        await asyncio.sleep(settings.request_delay)

    logger.info("Завершено выполнение задачи: Удаление сделок 'Победители' с ответственным 'НЕРАЗОБРАННЫЕ ЗАЯВКИ'.")


async def optimize_accreditation_and_participants_stages(amo_client: AmoClient):
    """
    Обрабатывает сделки на этапах 'Аккредитация' и 'Участники' и удаляет эти этапы.
    Сделки с ответственным 'НЕРАЗОБРАННЫЕ ЗАЯВКИ' удаляются, остальные перемещаются на 'Холодные заявки'.
    """
    logger.info("Начало выполнения задачи: Оптимизация этапов 'Аккредитация' и 'Участники'.")

    # Эта задача зависит от ID воронки, этапов Аккредитация, Участники, Холодные заявки и пользователя НЕРАЗОБРАННЫЕ ЗАЯВКИ
    if not all([PIPELINE_ID_GOV_ORDER, STAGE_ID_ACCREDITATION, STAGE_ID_PARTICIPANTS, STAGE_ID_COLD_LEADS, USER_ID_UNASSIGNED]):
        logger.error("Не удалось выполнить оптимизацию этапов: не все необходимые ID сущностей известны.")
        return

    stages_to_process = {
        STAGE_ID_ACCREDITATION: STAGE_NAME_ACCREDITATION,
        STAGE_ID_PARTICIPANTS: STAGE_NAME_PARTICIPANTS
    }

    for stage_id, stage_name in stages_to_process.items():
        # Проверяем, что ID этапа существует перед попыткой обработки
        if stage_id is None:
            logger.warning(f"Пропуск обработки этапа '{stage_name}': ID этапа не найден.")
            continue

        logger.info(f"Начало обработки сделок на этапе '{stage_name}' ({stage_id}) в воронке '{PIPELINE_NAME_GOV_ORDER}' ({PIPELINE_ID_GOV_ORDER}).")

        # Получаем все сделки на текущем этапе в нужной воронке
        leads_on_stage = await amo_client.get_leads(
            pipeline_id=PIPELINE_ID_GOV_ORDER,
            status_id=stage_id
        )

        if not leads_on_stage:
            logger.info(f"На этапе '{stage_name}' ({stage_id}) сделок не найдено.")
        else:
            logger.info(f"Найдено {len(leads_on_stage)} сделок на этапе '{stage_name}'.")
            deals_to_move = []
            deals_to_delete = []

            # Распределяем сделки по спискам для перемещения или удаления
            for lead in leads_on_stage:
                lead_id = lead.get('id')
                responsible_user_id = lead.get('responsible_user_id')
                if responsible_user_id == USER_ID_UNASSIGNED:
                    deals_to_delete.append(lead_id)
                else:
                    deals_to_move.append(lead_id)

            # Удаляем сделки с ответственным 'НЕРАЗОБРАННЫЕ ЗАЯВКИ'
            if deals_to_delete:
                logger.info(f"Удаление {len(deals_to_delete)} сделок с ответственным '{RESPONSIBLE_USER_NAME_UNASSIGNED}' на этапе '{stage_name}'.")
                for lead_id in deals_to_delete:
                     success = await amo_client.delete_lead(lead_id)
                     if success:
                        logger.info(f"Сделка ID={lead_id} успешно удалена.")
                     else:
                        logger.error(f"Ошибка при удалении сделки ID={lead_id}.")
                     await asyncio.sleep(settings.request_delay)
            else:
                logger.info(f"Нет сделок с ответственным '{RESPONSIBLE_USER_NAME_UNASSIGNED}' на этапе '{stage_name}' для удаления.")

            # Перемещаем остальные сделки на этап 'Холодные заявки'
            if deals_to_move:
                logger.info(f"Перемещение {len(deals_to_move)} сделок на этап '{STAGE_NAME_COLD_LEADS}' ({STAGE_ID_COLD_LEADS}).")
                update_data = [{'id': lead_id, 'status_id': STAGE_ID_COLD_LEADS} for lead_id in deals_to_move]
                updated_leads = await amo_client.update_leads_batch(update_data)
                if updated_leads:
                    logger.info(f"Успешно перемещено {len(updated_leads)} сделок.")
                else:
                    logger.error("Ошибка при массовом перемещении сделок.")
                await asyncio.sleep(settings.request_delay)
            else:
                logger.info(f"Нет сделок для перемещения с этапа '{stage_name}'.")

        logger.info(f"Завершено обработка сделок на этапе '{stage_name}'.")

    # После обработки сделок, удаляем сами этапы
    logger.info("Начало удаления этапов 'Аккредитация' и 'Участники'.")
    for stage_id, stage_name in stages_to_process.items():
        # Проверяем, что ID этапа существует перед попыткой удаления
        if stage_id is None:
            logger.warning(f"Пропуск удаления этапа '{stage_name}': ID этапа не найден.")
            continue

        logger.info(f"Попытка удаления этапа '{stage_name}' ({stage_id}).")
        leads_remaining = await amo_client.get_leads(
            pipeline_id=PIPELINE_ID_GOV_ORDER,
            status_id=stage_id
        )
        if leads_remaining:
             logger.error(f"Не удалось удалить этап '{stage_name}' ({stage_id}): на нем остались сделки ({len(leads_remaining)}).")
        else:
            success = await amo_client.delete_pipeline_status(PIPELINE_ID_GOV_ORDER, stage_id)
            if success:
                logger.info(f"Этап '{stage_name}' ({stage_id}) успешно удален.")
            else:
                logger.error(f"Ошибка при удалении этапа '{stage_name}' ({stage_id}).")
        await asyncio.sleep(settings.request_delay)

    logger.info("Завершено выполнение задачи: Оптимизация этапов 'Аккредитация' и 'Участники'.")

# Код функции merge_negotiation_stages тоже должен быть в этом файле,
# используя глобальные переменные этапов переговоров (PRIMARY_NEGOTIATIONS, LPR_NEGOTIATIONS, NEGOTIATIONS)
# и AmoClient.
# ... код merge_negotiation_stages ...

# Если функция merge_negotiation_stages ссылается на USER_ID_ALENA или USER_ID_NOVIKOVA
# в логике назначения ответственного, тебе нужно будет решить, как использовать
# USER_ID_ILYA и USER_ID_RABOTNIK вместо них или в дополнение к ним,
# в зависимости от точных условий ТЗ для назначения ответственных.
# Например, если по ТЗ "Алена" и "Новикова Евгения" должны были получать сделки
# с определенных условий, а теперь вместо них должны получать "Илья" и "Работник",
# то в логике назначения ответственных нужно использовать USER_ID_ILYA и USER_ID_RABOTNIK.