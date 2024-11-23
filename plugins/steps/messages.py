from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(telegram_conn_id='test',
                        token='7558571068:AAHjqzzzV1OXG6kJj9wB3EicEeN2nYknatA',
                        chat_id='7558571068')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '{вставьте ваш chat_id}',
        'text': message
    }) # отправление сообщения 

def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id='test',
                        token='7558571068:AAHjqzzzV1OXG6kJj9wB3EicEeN2nYknatA',
                        chat_id='7558571068')
    
    dag = context['dag']
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    
    message = f'Исполнение DAG {dag} с id={run_id} и c task_instance {task_instance_key_str} было провалено!' # определение текста сообщения
    hook.send_message({
        'chat_id': '7558571068',
        'text': message
    }) # отправление сообщения 
   