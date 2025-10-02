# report_services/service_hiring_service.py
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class ServiceHiringService:
    def __init__(self, data_repository):
        self.repo = data_repository
    
    def generate_report(self, service_name):
        """Сгенерировать анализ найма для сервиса"""
        try:
            data = self.repo.get_service_hiring_analysis(service_name)
            
            # Диагностика данных
            logger.info(f"Анализ найма для {service_name}: basic_stats={len(data['basic_stats'])}, hiring_stats={len(data['hiring_stats'])}")
            
            if len(data['basic_stats']) == 0 or data['basic_stats'].iloc[0]['total_employees'] == 0:
                return f"❌ Сервис '{service_name}' не найден или нет данных", None
            
            basic_stats = data['basic_stats'].iloc[0]
            
            # Обрабатываем hiring_stats - может быть пустым
            if len(data['hiring_stats']) > 0:
                hiring_stats = data['hiring_stats'].iloc[0]
                monthly_fires = hiring_stats.get('monthly_fires', 0)
            else:
                monthly_fires = 0
                logger.warning(f"Нет данных по увольнениям для сервиса {service_name}")
            
            # Рассчитываем показатели
            hiring_needed = (monthly_fires / 2) * 1.1  # Учет сезонности +10%
            
            total_employees = basic_stats['total_employees']
            total_attrition = (basic_stats['total_fires'] / total_employees) * 100 if total_employees > 0 else 0
            hiring_percentage = (hiring_needed / total_employees) * 100 if total_employees > 0 else 0
            
            # Получаем данные по текучести по возрастам для этого сервиса
            last_date = self.repo.get_last_report_date()
            age_attrition_data = self.repo.get_detailed_service_analysis(service_name, last_date)
            
            # Форматируем отчет
            report_text = self._format_report(
                service_name, basic_stats, monthly_fires, 
                hiring_needed, total_attrition, hiring_percentage,
                age_attrition_data
            )
            
            return report_text, None
            
        except Exception as e:
            logger.error(f"Ошибка анализа найма для сервиса {service_name}: {e}", exc_info=True)
            return f"❌ Ошибка анализа найма: {str(e)}", None
    
    def _format_report(self, service_name, basic_stats, monthly_fires, 
                      hiring_needed, total_attrition, hiring_percentage,
                      age_attrition_data):
        """Форматирование отчета по найму"""
        text = f"🔍 АНАЛИЗ НАЙМА: {service_name}\n\n"
        
        text += f"📊 ОБЩАЯ СТАТИСТИКА:\n"
        text += f"• Сотрудников: {int(basic_stats['total_employees']):,} чел.\n"
        text += f"• Текучесть: {total_attrition:.1f}%\n"
        text += f"• Средний возраст: {basic_stats['avg_age']:.1f} лет\n"
        text += f"• Средний опыт: {basic_stats['avg_experience']:.1f} мес.\n\n"
        
        text += f"🎯 РЕКОМЕНДАЦИИ ПО НАЙМУ:\n"
        text += f"• Увольнений за Июль-Август: {int(monthly_fires)} чел.\n"
        text += f"• Среднемесячные увольнения: {monthly_fires/2:.1f} чел./мес\n"
        text += f"• Рекомендуемый найм: {hiring_needed:.1f} чел./мес\n"
        text += f"• Доля от штата: {hiring_percentage:.1f}%\n\n"
        
        if len(age_attrition_data) > 0:
            age_attrition = age_attrition_data.groupby('age_category', as_index=False).agg({
                'employees': 'sum',
                'fires': 'sum'
            })
            age_attrition['attrition_rate'] = (age_attrition['fires'] / age_attrition['employees']) * 100
            age_attrition = age_attrition[age_attrition['employees'] > 0].sort_values('attrition_rate', ascending=False)
            
            if len(age_attrition) > 0:
                text += "📈 ТЕКУЧЕСТЬ ПО ВОЗРАСТУ:\n"
                for _, row in age_attrition.iterrows():
                    text += f"• {row['age_category']}: {row['attrition_rate']:.1f}% ({int(row['fires'])} ув.)\n"
                text += "\n"
        
        text += "💡 РЕКОМЕНДАЦИИ:\n"
        
        # Приоритет найма на основе процента от штата
        if hiring_percentage > 10:
            text += "• 🔴 СРОЧНЫЙ НАЙМ: Очень высокая потребность (>10% штата)\n"
        elif hiring_percentage > 5:
            text += "• 🟠 ВЫСОКИЙ ПРИОРИТЕТ: Значительная потребность (5-10% штата)\n"
        elif hiring_percentage > 2:
            text += "• 🟡 СРЕДНИЙ ПРИОРИТЕТ: Умеренная потребность (2-5% штата)\n"
        elif hiring_needed > 0:
            text += "• 🟢 НИЗКИЙ ПРИОРИТЕТ: Небольшая потребность (<2% штата)\n"
        else:
            text += "• 🟢 СТАБИЛЬНАЯ СИТУАЦИЯ: Потребность в найме отсутствует\n"
        
        # Оценка текучести
        if total_attrition > 15:
            text += "• 🔴 ВЫСОКАЯ ТЕКУЧЕСТЬ: >15% - требуется анализ причин увольнений\n"
        elif total_attrition > 10:
            text += "• 🟠 ПОВЫШЕННАЯ ТЕКУЧЕСТЬ: 10-15% - внимание к retention\n"
        elif total_attrition > 5:
            text += "• 🟡 УМЕРЕННАЯ ТЕКУЧЕСТЬ: 5-10% - в пределах нормы\n"
        else:
            text += "• 🟢 НИЗКАЯ ТЕКУЧЕСТЬ: <5% - отличный показатель\n"
        
        # Дополнительные рекомендации
        if hiring_needed > 20:
            text += "• 📋 Рекомендуется: Массовый рекрутинг + аутсорсинг\n"
        elif hiring_needed > 10:
            text += "• 📋 Рекомендуется: Активный поиск + реферальная программа\n"
        elif hiring_needed > 5:
            text += "• 📋 Рекомендуется: Стандартный рекрутинг + LinkedIn\n"
        elif hiring_needed > 0:
            text += "• 📋 Рекомендуется: Точечный поиск кандидатов\n"
        
        text += "\n📊 Основано на данных за Июль-Август 2025\n"
        text += "💡 Учтена сезонность (+10% к средним показателям)\n"
        
        return text
    
    def get_hiring_priority(self, hiring_needed, total_employees):
        """Определение приоритета найма"""
        if total_employees == 0:
            return "неопределенный"
        
        hiring_percentage = (hiring_needed / total_employees) * 100
        
        if hiring_percentage > 10:
            return "критический"
        elif hiring_percentage > 5:
            return "высокий"
        elif hiring_percentage > 2:
            return "средний"
        elif hiring_needed > 0:
            return "низкий"
        else:
            return "отсутствует"