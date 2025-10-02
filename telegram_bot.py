# telegram_bot.py
import asyncio
import re
import os
import time
import logging
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler, CallbackQueryHandler
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from menu.advanced_core import AdvancedHRAnalyzer
from config import DB_PATH, BOT_TOKEN
from ai_assistant import AIAssistant
from menu.core_handlers import *
from menu.context import AnalysisContext

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SELECT_ACTION, SELECT_DETAILED, SELECT_HIRING_SERVICE, AI_ASSISTANT = range(4)

class HRTelegramBot:
    def __init__(self):
        self.analyzer = AdvancedHRAnalyzer(DB_PATH)
        self.ai_assistant = AIAssistant(DB_PATH)
        self.application = Application.builder().token(BOT_TOKEN).build()
        self.menu_commands = [
            "📈 Динамика компании", "👥 Демография", "🌐 Анализ сервисов", 
            "⚠️  Оценка рисков", "🎯 Рекомендации по найму", "🔍 Детальный анализ",
            "💬 Спросить Эйчарика"
        ]
        
        self.analysis_ctx = AnalysisContext(
            analyzer=self.analyzer,
            menu_commands=self.menu_commands,
            show_main_menu_func=self.show_main_menu,
            select_detailed_const=SELECT_DETAILED,
            select_hiring_const=SELECT_HIRING_SERVICE,
            end_const=ConversationHandler.END
        )
        
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        context.user_data.clear()
        return await self.show_main_menu(update, MAIN_MENU_TEXT)

    async def show_main_menu(self, update: Update, text: str = MAIN_MENU_TEXT):
        reply_markup = create_main_keyboard()
        
        if hasattr(update, 'message'):
            await update.message.reply_text(text, reply_markup=reply_markup)
        else:
            await update.reply_text(text, reply_markup=reply_markup)
        
        return SELECT_ACTION

    async def get_all_services(self):
        try:
            return self.analyzer.get_all_services()
        except Exception as e:
            logger.error(f"Ошибка при получении списка сервисов: {e}")
            return ["Такси", "Маркет", "Крауд", "Лавка", "Финтех", "Еда", "Доставка", "Облако"]

    async def handle_callback_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        if query.data == 'back_to_menu':
            context.user_data.clear()
            try:
                await query.message.delete()
            except Exception as e:
                logger.error(f"Ошибка удаления сообщения: {e}")
            return await self.show_main_menu(query, RETURN_TO_MENU_TEXT)
        
        elif query.data.startswith('choose_another_'):
            analysis_type = query.data.replace('choose_another_', '')
            
            if analysis_type == 'hiring':
                services = list(context.user_data.get('hiring_data', {}).keys())
                keyboard = create_service_selection_keyboard(services, 'hiring')
                await query.message.reply_text(CHOOSE_HIRING_SERVICE_TEXT, reply_markup=keyboard)
                context.user_data['conversation_state'] = 'awaiting_hiring_service'
                return SELECT_HIRING_SERVICE
            else:
                services = await self.get_all_services()
                keyboard = create_service_selection_keyboard(services, 'detailed')
                await query.message.reply_text(CHOOSE_SERVICE_TEXT, reply_markup=keyboard)
                context.user_data['conversation_state'] = 'awaiting_service'
                return SELECT_DETAILED
        
        elif query.data.startswith('back_to_actions_'):
            analysis_type = query.data.replace('back_to_actions_', '')
            action_keyboard = create_next_action_keyboard(analysis_type)
            
            try:
                await query.message.delete()
            except:
                pass
                
            await query.message.reply_text(
                NEXT_ACTION_TEXT,
                reply_markup=action_keyboard
            )
            return SELECT_ACTION
    
        elif query.data.startswith('service_'):
            return await handle_callback_service_selection(query, context, self.analyzer, SELECT_ACTION)
        
        elif query.data.startswith('graphs_'):
            await query.message.delete()
            return await send_graphs(query, context)
        
        elif query.data == 'choose_another':
            current_analysis_type = context.user_data.get('last_analysis_type')
            
            if current_analysis_type == 'hiring':
                services = list(context.user_data.get('hiring_data', {}).keys())
                keyboard = create_service_selection_keyboard(services, 'hiring')
                await query.message.reply_text(CHOOSE_HIRING_SERVICE_TEXT, reply_markup=keyboard)
                context.user_data['conversation_state'] = 'awaiting_hiring_service'
                return SELECT_HIRING_SERVICE
            else:
                services = await self.get_all_services()
                keyboard = create_service_selection_keyboard(services, 'detailed')
                await query.message.reply_text(CHOOSE_SERVICE_TEXT, reply_markup=keyboard)
                context.user_data['conversation_state'] = 'awaiting_service'
                return SELECT_DETAILED

    async def handle_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_text = update.message.text
        
        # Обработка команды возврата в меню - ДОБАВЛЕНА КНОПКА "◀️ В меню"
        if user_text in ["◀️ Главное меню", "/menu", "меню", "◀️ В меню"]:
            context.user_data.clear()
            return await self.show_main_menu(update, RETURN_TO_MENU_TEXT)
        
        # ПРИОРИТЕТ 1: Если это команда из меню - обрабатываем как действие
        if user_text in self.menu_commands:
            try:
                if user_text in ["📈 Динамика компании", "👥 Демография", "🌐 Анализ сервисов"]:
                    loading_message_id = await send_loading_message(update)
                    await show_typing(update)
                    
                    result = None
                    plot_path = None
                    
                    if user_text == "📈 Динамика компании":
                        result, plot_path = self.analyzer.company_dynamics()
                    elif user_text == "👥 Демография":
                        result, plot_path = self.analyzer.demographic_dashboard()
                    elif user_text == "🌐 Анализ сервисов":
                        result, plot_path = self.analyzer.service_analysis()
                    
                    if result is None:
                        await update.message.reply_text("❌ Не удалось получить данные для анализа")
                        return await self.show_main_menu(update)
                    
                    context.user_data['plot_path'] = plot_path
                    context.user_data['last_analysis_type'] = user_text.lower().replace(' ', '_')
                    
                    await send_analysis_result(update, result, None, loading_message_id)
                    
                    graphs_keyboard = create_graphs_keyboard("main")
                    await update.message.reply_text(
                        "Хотите увидеть графики?",
                        reply_markup=graphs_keyboard
                    )
                    
                    return SELECT_ACTION
                
                elif user_text == "💬 Спросить Эйчарика":
                    await update.message.reply_text(
                        "Привет, я Эйчарик! Помогу разобраться в HR аналитике 📊\n\n"
                        "Задай любой интересующий вопрос, например:\n"
                        "• Сколько сотрудников в Такси?\n"
                        "• Покажи динамику наймов\n"
                        "• Какая текучесть в Еде?\n\n"
                        "❓ Если хочешь узнать какие еще можно задать вопросы, то напиши:\n"
                        "• Примеры вопросов\n\n"
                        "Для возврата в меню нажмите кнопку ️◀️ Главное меню",
                        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◀️ Главное меню")]], resize_keyboard=True)
                    )
                    return AI_ASSISTANT
                
                else:
                    loading_message_id = await send_loading_message(update)
                    await show_typing(update)
                    
                    if user_text == "⚠️  Оценка рисков":
                        result, plot_path = self.analyzer.risk_assessment()
                        if result is None:
                            await update.message.reply_text("❌ Не удалось оценить риски")
                            return await self.show_main_menu(update)
                            
                        await send_analysis_result(update, result, plot_path, loading_message_id)
                        return await self.show_main_menu(update, NEXT_ACTION_TEXT)
                        
                    elif user_text == "🎯 Рекомендации по найму":
                        result, hiring_data = self.analyzer.hiring_recommendations()
                        
                        if hiring_data is None or not hiring_data:
                            await send_analysis_result(update, "❌ Нет данных для рекомендаций по найму", None, loading_message_id)
                            return await self.show_main_menu(update, NEXT_ACTION_TEXT)
                        
                        context.user_data['hiring_data'] = hiring_data
                        context.user_data['last_analysis_type'] = 'hiring'
                        
                        await send_analysis_result(update, result, None, loading_message_id)
                        
                        services = list(hiring_data.keys())
                        keyboard = create_service_selection_keyboard(services, 'hiring')
                        
                        await update.message.reply_text(
                            "📋 Выберите сервис для подробного анализа найма:",
                            reply_markup=keyboard
                        )
                        
                        context.user_data['conversation_state'] = 'awaiting_hiring_service'
                        return SELECT_HIRING_SERVICE
                        
                    elif user_text == "🔍 Детальный анализ":
                        services = await self.get_all_services()
                        keyboard = create_service_selection_keyboard(services, 'detailed')
                        
                        # ИСПРАВЛЕНИЕ: Убираем лишнее сообщение
                        if loading_message_id:
                            await delete_message(update, loading_message_id)
                        
                        await update.message.reply_text(
                            "📋 Выберите сервис для детального анализа:",
                            reply_markup=keyboard
                        )
                        context.user_data['conversation_state'] = 'awaiting_service'
                        context.user_data['last_analysis_type'] = 'detailed'
                        return SELECT_DETAILED
                
                return SELECT_ACTION
                
            except Exception as e:
                logger.error(f"Ошибка обработки действия: {e}")
                logger.error(f"Трассировка ошибки:", exc_info=True)
                await update.message.reply_text(f"❌ Произошла ошибка: {str(e)}")
                return await self.show_main_menu(update)
        
        # ПРИОРИТЕТ 2: Проверяем текущее состояние конверсации
        current_state = context.user_data.get('conversation_state')
        
        if current_state == 'awaiting_service':
            return await handle_detailed_analysis(update, context, self.analysis_ctx)
        elif current_state == 'awaiting_hiring_service':
            return await handle_hiring_service_analysis(update, context, self.analysis_ctx)
        
        # ПРИОРИТЕТ 3: Если это не команда меню и не состояние конверсации - показываем подсказку
        else:
            # Если пользователь отправил текст, но это не команда - показываем меню
            if user_text.strip() and user_text not in self.menu_commands:
                await update.message.reply_text(
                    "💡 Я бот для HR аналитики! Пожалуйста, выберите действие из меню ниже.\n\n"
                    "Если хотите задать вопрос, нажмите '💬 Спросить Эйчарика'"
                )
            return await self.show_main_menu(update, CHOOSE_FROM_MENU_TEXT)

    async def handle_ai_assistant(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_text = update.message.text
        
        # Обработка возврата в меню из AI режима
        if user_text in ["◀️ Главное меню", "/menu", "меню", "◀️ В меню"]:
            context.user_data.clear()
            return await self.show_main_menu(update, RETURN_TO_MENU_TEXT)
        
        current_time = time.time()
        last_request_time = context.user_data.get('last_ai_request', 0)
        
        if current_time - last_request_time < 2:
            await update.message.reply_text("⏳ Пожалуйста, подождите немного перед следующим запросом")
            return AI_ASSISTANT
        
        context.user_data['last_ai_request'] = current_time
        
        try:
            loading_message_id = await send_loading_message(update)
            await show_typing(update)
            
            response = await self.ai_assistant.process_query(user_text)
            
            await delete_message(update, loading_message_id)
            await update.message.reply_text(response)
            
            return AI_ASSISTANT
            
        except Exception as e:
            logger.error(f"Ошибка обработки AI запроса: {e}")
            await update.message.reply_text(
                f"❌ Ошибка обработки запроса: {str(e)}\n\n"
                "Попробуйте переформулировать вопрос или вернитесь в меню: /menu"
            )
            return AI_ASSISTANT

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        context.user_data.clear()
        await update.message.reply_text("Действие отменено.")
        return await self.show_main_menu(update)

    def run(self):
        callback_handler = CallbackQueryHandler(self.handle_callback_query)
        
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler('start', self.start)],
            states={
                SELECT_ACTION: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_action),
                    callback_handler
                ],
                SELECT_DETAILED: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND,
                        lambda u, c: handle_detailed_analysis(u, c, self.analysis_ctx)),
                    callback_handler
                ],
                SELECT_HIRING_SERVICE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND,
                        lambda u, c: handle_hiring_service_analysis(u, c, self.analysis_ctx)),
                    callback_handler
                ],
                AI_ASSISTANT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_ai_assistant),
                    callback_handler
                ]
            },
            fallbacks=[CommandHandler('cancel', self.cancel)],
            allow_reentry=True
        )
        
        self.application.add_handler(CommandHandler('menu', self.show_main_menu))
        self.application.add_handler(conv_handler)
        
        logger.info("HR бот c Эйчариком запущен 💡")
        self.application.run_polling()

if __name__ == "__main__":
    bot = HRTelegramBot()
    bot.run()
