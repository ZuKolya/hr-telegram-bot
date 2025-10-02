# core_handlers.py
import asyncio
import os
import logging
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction
from telegram.ext import ContextTypes

MAIN_MENU_TEXT = "📈 HR Бот запущен\n\nВыберите тип анализа или задайте вопрос Эйчарику💡:"
CHOOSE_SERVICE_TEXT = "Выберите сервис для детального анализа:"
CHOOSE_HIRING_SERVICE_TEXT = "Выберите сервис для подробного анализа найма:"
NEXT_ACTION_TEXT = "Выберите следующее действие:"
RETURN_TO_MENU_TEXT = "Возврат в главное меню"
CHOOSE_FROM_MENU_TEXT = "Выберите действие из меню:"
LOADING_TEXT = "⏳ Загружаю данные... Это может занять несколько секунд"

logger = logging.getLogger(__name__)

async def show_typing(update: Update):
    try:
        if hasattr(update, 'message'):
            await update.message.chat.send_action(action=ChatAction.TYPING)
        else:
            await update.chat.send_action(action=ChatAction.TYPING)
    except Exception as e:
        logger.error(f"Ошибка индикатора typing: {e}")

async def send_loading_message(update: Update):
    try:
        if hasattr(update, 'message'):
            message = await update.message.reply_text(LOADING_TEXT)
            return message.message_id
        else:
            message = await update.reply_text(LOADING_TEXT)
            return message.message_id
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения загрузки: {e}")
        return None

async def delete_message(update: Update, message_id: int):
    try:
        if hasattr(update, 'message'):
            await update.message.chat.delete_message(message_id)
        else:
            await update.delete_message(message_id)
    except Exception as e:
        logger.error(f"Ошибка удаления сообщения: {e}")

async def send_analysis_result(update: Update, result: str, plot_path=None, loading_message_id=None):
    if loading_message_id:
        await delete_message(update, loading_message_id)
    
    if len(result) > 4000:
        parts = [result[i:i+4000] for i in range(0, len(result), 4000)]
        for i, part in enumerate(parts):
            await update.message.reply_text(f"({i+1}/{len(parts)})\n{part}")
            await asyncio.sleep(0.5)
    else:
        await update.message.reply_text(result)
    
    if plot_path:
        try:
            if isinstance(plot_path, list):
                for path in plot_path:
                    if path and os.path.exists(path):
                        await show_typing(update)
                        with open(path, 'rb') as photo:
                            await update.message.reply_photo(photo=photo)
                        os.remove(path)
                        await asyncio.sleep(0.5)
            elif plot_path and os.path.exists(plot_path):
                await show_typing(update)
                with open(plot_path, 'rb') as photo:
                    await update.message.reply_photo(photo=photo)
                os.remove(plot_path)
        except Exception as e:
            logger.error(f"Ошибка при отправке/удалении графиков: {e}")

def create_main_keyboard():
    keyboard = [
        [KeyboardButton("👥 Демография"), KeyboardButton("🌐 Анализ сервисов")],
        [KeyboardButton("📈 Динамика компании"), KeyboardButton("⚠️  Оценка рисков")],
        [KeyboardButton("🎯 Рекомендации по найму"), KeyboardButton("🔍 Детальный анализ")],
        [KeyboardButton("💬 Спросить Эйчарика")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def create_graphs_keyboard(analysis_type):
    keyboard = [
        [InlineKeyboardButton("📊 Показать графики", callback_data=f"graphs_{analysis_type}")],
        [InlineKeyboardButton("◀️ Вернуться в меню", callback_data="back_to_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_service_keyboard(services, include_back=False):
    keyboard = []
    row = []
    
    for i, service in enumerate(services):
        row.append(InlineKeyboardButton(service, callback_data=f"service_{service}"))
        if len(row) == 2 or i == len(services) - 1:
            keyboard.append(row)
            row = []
    
    if include_back:
        keyboard.append([InlineKeyboardButton("◀️ В главное меню", callback_data="back_to_menu")])
    
    return InlineKeyboardMarkup(keyboard)

def create_next_action_keyboard(analysis_type):
    keyboard = [
        [InlineKeyboardButton("📋 Выбрать другой сервис", callback_data=f"choose_another_{analysis_type}")],
        [InlineKeyboardButton("◀️ В главное меню", callback_data="back_to_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_service_selection_keyboard(services, analysis_type):
    keyboard = []
    row = []
    
    for i, service in enumerate(services):
        row.append(InlineKeyboardButton(service, callback_data=f"service_{service}"))
        if len(row) == 2 or i == len(services) - 1:
            keyboard.append(row)
            row = []
    
    keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data=f"back_to_actions_{analysis_type}")])
    
    return InlineKeyboardMarkup(keyboard)

async def handle_detailed_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE, analysis_ctx):
    user_text = update.message.text
    
    # Если пользователь нажал кнопку возврата в меню
    if user_text in ["◀️ Главное меню", "/menu", "меню"]:
        context.user_data.clear()
        return await analysis_ctx.show_main_menu_func(update, RETURN_TO_MENU_TEXT)
    
    # Если пользователь выбрал команду из меню - возвращаем в главное меню
    if user_text in analysis_ctx.menu_commands:
        context.user_data.clear()
        return await analysis_ctx.show_main_menu_func(update, RETURN_TO_MENU_TEXT)
    
    loading_message_id = None
    try:
        loading_message_id = await send_loading_message(update)
        await show_typing(update)
        
        result, plot_path = analysis_ctx.analyzer.detailed_analysis("service", user_text)
        
        if result.startswith("❌ Сервис"):
            if loading_message_id:
                await delete_message(update, loading_message_id)
            await update.message.reply_text(result)
            return analysis_ctx.select_detailed_const
        
        context.user_data['plot_path'] = plot_path
        context.user_data['last_analysis_type'] = 'detailed'
        
        await send_analysis_result(update, result, None, loading_message_id)
        
        action_keyboard = create_next_action_keyboard("detailed")
        await update.message.reply_text(
            NEXT_ACTION_TEXT,
            reply_markup=action_keyboard
        )
        
        return SELECT_ACTION
        
    except Exception as e:
        if loading_message_id:
            await delete_message(update, loading_message_id)
        await update.message.reply_text(f"❌ Ошибка анализа: {str(e)}")
        return analysis_ctx.select_detailed_const

async def handle_hiring_service_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE, analysis_ctx):
    user_text = update.message.text
    
    # Если пользователь нажал кнопку возврата в меню
    if user_text in ["◀️ Главное меню", "/menu", "меню"]:
        context.user_data.clear()
        return await analysis_ctx.show_main_menu_func(update, RETURN_TO_MENU_TEXT)
    
    # Если пользователь выбрал команду из меню - возвращаем в главное меню
    if user_text in analysis_ctx.menu_commands:
        context.user_data.clear()
        return await analysis_ctx.show_main_menu_func(update, RETURN_TO_MENU_TEXT)
    
    loading_message_id = None
    try:
        loading_message_id = await send_loading_message(update)
        await show_typing(update)
        
        result, plot_path = analysis_ctx.analyzer.hiring_service_analysis(user_text)
        
        if result.startswith("❌ Сервис"):
            if loading_message_id:
                await delete_message(update, loading_message_id)
            await update.message.reply_text(result)
            return analysis_ctx.select_hiring_const
        
        context.user_data['plot_path'] = plot_path
        context.user_data['last_analysis_type'] = 'hiring'
        
        await send_analysis_result(update, result, None, loading_message_id)
        
        action_keyboard = create_next_action_keyboard("hiring")
        await update.message.reply_text(
            NEXT_ACTION_TEXT,
            reply_markup=action_keyboard
        )
        
        return SELECT_ACTION
        
    except Exception as e:
        if loading_message_id:
            await delete_message(update, loading_message_id)
        await update.message.reply_text(f"❌ Ошибка анализа найма: {str(e)}")
        return analysis_ctx.select_hiring_const

async def handle_callback_service_selection(query, context, analyzer, end_const):
    service_name = query.data.replace('service_', '')
    
    loading_message_id = None
    try:
        loading_message = await query.message.reply_text(LOADING_TEXT)
        loading_message_id = loading_message.message_id
        await show_typing(query)
        
        result = None
        plot_path = None
        
        if context.user_data.get('conversation_state') == 'awaiting_service':
            result, plot_path = analyzer.detailed_analysis("service", service_name)
            analysis_type = 'detailed'
            
        elif context.user_data.get('conversation_state') == 'awaiting_hiring_service':
            result, plot_path = analyzer.hiring_service_analysis(service_name)
            analysis_type = 'hiring'
        
        if result is None:
            raise Exception("Результат анализа не получен")
        
        if result.startswith("❌ Сервис"):
            await delete_message(query, loading_message_id)
            await query.message.reply_text(result)
            return context.user_data.get('conversation_state')
        
        context.user_data['plot_path'] = plot_path
        context.user_data['last_analysis_type'] = analysis_type
        
        await delete_message(query, loading_message_id)
        await query.message.reply_text(result)
        
        action_keyboard = create_next_action_keyboard(analysis_type)
        await query.message.reply_text(
            NEXT_ACTION_TEXT,
            reply_markup=action_keyboard
        )
        
        return end_const
        
    except Exception as e:
        if loading_message_id:
            await delete_message(query, loading_message_id)
        logger.error(f"Ошибка при обработке сервиса {service_name}: {e}")
        await query.message.reply_text(f"❌ Произошла ошибка при анализе сервиса '{service_name}'.")
        return context.user_data.get('conversation_state')

async def send_graphs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        plot_path = context.user_data.get('plot_path')
        if plot_path:
            await show_typing(update)
            
            if isinstance(plot_path, list):
                for path in plot_path:
                    if path and os.path.exists(path):
                        with open(path, 'rb') as photo:
                            await update.message.reply_photo(photo=photo)
                        os.remove(path)
                        await asyncio.sleep(0.5)
            elif plot_path and os.path.exists(plot_path):
                with open(plot_path, 'rb') as photo:
                    await update.message.reply_photo(photo=photo)
                os.remove(plot_path)
            
            context.user_data.pop('plot_path', None)
            
            # ИСПРАВЛЕНИЕ: Возвращаем правильную клавиатуру для возврата в меню
            await update.message.reply_text(
                "✅ Графики загружены!",
                reply_markup=create_main_keyboard()  # ← ВОЗВРАЩАЕМ ОСНОВНУЮ КЛАВИАТУРУ
            )
            return SELECT_ACTION
            
        else:
            await update.message.reply_text(
                "❌ Графики не найдены или уже были показаны",
                reply_markup=create_main_keyboard()
            )
            return SELECT_ACTION
            
    except Exception as e:
        logger.error(f"Ошибка при отправке графиков: {e}")
        await update.message.reply_text(
            "❌ Ошибка при загрузке графиков",
            reply_markup=create_main_keyboard()
        )
        return SELECT_ACTION

SELECT_ACTION, SELECT_DETAILED, SELECT_HIRING_SERVICE, AI_ASSISTANT = range(4)