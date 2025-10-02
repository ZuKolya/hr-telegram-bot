# report_services/hiring_recommendations_service.py
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class HiringRecommendationsService:
    def __init__(self, data_repository):
        self.repo = data_repository
    
    def generate_report(self):
        """Сгенерировать рекомендации по найму"""
        try:
            data = self.repo.get_hiring_recommendations_data()
            
            # Диагностика данных
            logger.info(f"Данные для рекомендаций по найму: {len(data)} строк")
            if len(data) > 0:
                logger.info(f"Колонки: {data.columns.tolist()}")
                logger.info(f"Данные: {data.to_dict('records')}")
            
            if len(data) == 0 or data.empty:
                return "❌ Нет данных для рекомендаций по найму (пустой результат запроса)", None
            
            # Проверяем наличие необходимых колонок
            required_columns = ['monthly_fires', 'total_employees']
            missing_columns = [col for col in required_columns if col not in data.columns]
            
            if missing_columns:
                return f"❌ Отсутствуют необходимые колонки: {missing_columns}. Доступные колонки: {data.columns.tolist()}", None
            
            # Проверяем, есть ли данные об увольнениях
            total_fires = data['monthly_fires'].sum()
            if total_fires == 0:
                return "📊 За последние 2 месяца увольнений не зафиксировано. Потребность в найме отсутствует.", {}
            
            # Рассчитываем потребность в найме (среднемесячные увольнения + 10% на сезонность)
            data['monthly_fires_avg'] = data['monthly_fires'] / 2  # Среднее за месяц
            data['hiring_needed'] = data['monthly_fires_avg'] * 1.1  # +10% на сезонность
            
            # Фильтруем сервисы с положительной потребностью
            valid_data = data[data['hiring_needed'] > 0]
            
            if len(valid_data) == 0:
                return "📊 На текущий момент потребность в найме отсутствует по всем сервисам", {}
            
            # Форматируем отчет
            report_text = self._format_report(valid_data)
            hiring_data = valid_data.set_index('service')['hiring_needed'].to_dict()
            
            return report_text, hiring_data
            
        except Exception as e:
            logger.error(f"Ошибка формирования рекомендаций: {e}", exc_info=True)
            return f"❌ Ошибка формирования рекомендаций: {str(e)}", None
    
    def _format_report(self, data):
        """Форматирование текста отчета"""
        total_hiring = data['hiring_needed'].sum()
        
        text = "🎯 РЕКОМЕНДАЦИИ ПО НАЙМУ\n\n"
        text += f"📈 Общая потребность в найме: {total_hiring:.1f} чел./мес\n"
        text += f"📊 Основано на данных за Июль-Август 2025\n\n"
        
        text += "🏢 Рекомендации по сервисам:\n"
        for _, row in data.iterrows():
            hiring_percentage = (row['hiring_needed'] / row['total_employees']) * 100 if row['total_employees'] > 0 else 0
            text += f"• {row['service']}: {row['hiring_needed']:.1f} чел./мес ({hiring_percentage:.1f}% от штата)\n"
        
        text += f"\n💡 Учтена сезонность (+10% к средним показателям)\n\n"
        
        # Анализ приоритетов
        text += "🎯 ПРИОРИТЕТЫ НАЙМА:\n"
        
        high_priority = data[data['hiring_needed'] > 100]
        medium_priority = data[(data['hiring_needed'] > 10) & (data['hiring_needed'] <= 100)]
        low_priority = data[data['hiring_needed'] <= 10]
        
        if len(high_priority) > 0:
            text += "• 🔴 ВЫСОКИЙ ПРИОРИТЕТ (>100 чел./мес):\n"
            for _, row in high_priority.iterrows():
                text += f"  - {row['service']}: {row['hiring_needed']:.1f} чел./мес\n"
        
        if len(medium_priority) > 0:
            text += "• 🟡 СРЕДНИЙ ПРИОРИТЕТ (10-100 чел./мес):\n"
            for _, row in medium_priority.iterrows():
                text += f"  - {row['service']}: {row['hiring_needed']:.1f} чел./мес\n"
        
        if len(low_priority) > 0:
            text += "• 🟢 НИЗКИЙ ПРИОРИТЕТ (≤10 чел./мес):\n"
            for _, row in low_priority.iterrows():
                text += f"  - {row['service']}: {row['hiring_needed']:.1f} чел./мес\n"
        
        # Статистика по увольнениям
        total_fires = data['monthly_fires'].sum()
        text += f"\n📊 СТАТИСТИКА УВОЛЬНЕНИЙ:\n"
        text += f"• Всего увольнений за 2 месяца: {total_fires:.0f} чел.\n"
        text += f"• Среднемесячные увольнения: {total_fires/2:.1f} чел./мес\n"
        text += f"• Сервисов с потребностью в найме: {len(data)} из {len(data)}\n"
        
        return text