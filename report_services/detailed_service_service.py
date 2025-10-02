# report_services/detailed_service_service.py
import pandas as pd

class DetailedServiceService:
    def __init__(self, data_repository):
        self.repo = data_repository
    
    def generate_report(self, service_name, report_date):
        """Сгенерировать детальный отчет по сервису"""
        data = self.repo.get_detailed_service_analysis(service_name, report_date)
        
        if len(data) == 0:
            return f"❌ Сервис '{service_name}' не найден или нет данных", None
        
        # Рассчитываем общую статистику
        total_employees = data['employees'].sum()
        total_fires = data['fires'].sum()
        total_attrition = (total_fires / total_employees) * 100 if total_employees > 0 else 0
        avg_age = (data['avg_age'] * data['employees']).sum() / total_employees if total_employees > 0 else 0
        avg_experience = (data['avg_experience'] * data['employees']).sum() / total_employees if total_employees > 0 else 0
        
        # Форматируем отчет
        report_text = self._format_report(
            service_name, data, total_employees, total_attrition, 
            avg_age, avg_experience, report_date
        )
        
        return report_text, None
    
    def _format_report(self, service_name, data, total_employees, total_attrition, 
                      avg_age, avg_experience, report_date):
        """Форматирование текста отчета"""
        text = f"🔍 ДЕТАЛЬНЫЙ АНАЛИЗ: {service_name}\n"
        text += f"📅 на {report_date[:10]}\n\n"
        
        text += f"📊 ОБЩАЯ СТАТИСТИКА:\n"
        text += f"• Сотрудников: {int(total_employees):,} чел.\n"
        text += f"• Текучесть: {total_attrition:.1f}%\n"
        text += f"• Средний возраст: {avg_age:.1f} лет\n"
        text += f"• Средний опыт: {avg_experience:.1f} мес.\n\n"
        
        # Распределение по возрасту
        age_data = data.groupby('age_category', as_index=False).agg({
            'employees': 'sum',
            'fires': 'sum',
            'avg_age': 'mean',
            'avg_experience': 'mean'
        })
        age_data['attrition_rate'] = (age_data['fires'] / age_data['employees']) * 100
        
        text += "👥 РАСПРЕДЕЛЕНИЕ ПО ВОЗРАСТУ:\n"
        for _, row in age_data.iterrows():
            text += f"\n• {row['age_category']}:\n"
            text += f"  👥 {int(row['employees']):,} чел. ({row['attrition_rate']:.1f}% текучести)\n"
            text += f"  📊 Средний возраст: {row['avg_age']:.1f} лет\n"
            text += f"  ⏳ Опыт работы: {row['avg_experience']:.1f} мес.\n"
        
        # Гендерный состав
        gender_data = data.groupby('sex', as_index=False)['employees'].sum()
        text += f"\n👫 ГЕНДЕРНЫЙ СОСТАВ:\n"
        for _, row in gender_data.iterrows():
            percentage = (row['employees'] / total_employees) * 100
            gender = "Женщины" if row['sex'] == 'F' else "Мужчины"
            text += f"• {gender}: {percentage:.1f}% ({int(row['employees']):,} чел.)\n"
        
        # Опыт работы - правильный порядок
        exp_data = data.groupby('experience_category', as_index=False)['employees'].sum()
        text += f"\n📅 РАСПРЕДЕЛЕНИЕ ПО ОПЫТУ:\n"
        
        # Правильный порядок сортировки для опыта
        exp_order = ['1 мес', '2 мес', '3 мес', 'до 1 года', '1-2 года', '2-3 года', '3-5 лет', 'более 5 лет']
        
        # Создаем временную колонку для сортировки
        exp_data['order'] = exp_data['experience_category'].apply(
            lambda x: exp_order.index(x) if x in exp_order else len(exp_order)
        )
        exp_data = exp_data.sort_values('order')
        
        for _, row in exp_data.iterrows():
            percentage = (row['employees'] / total_employees) * 100
            text += f"• {row['experience_category']}: {percentage:.1f}% ({int(row['employees']):,} чел.)\n"
        
        # Анализ рисков для сервиса
        text += f"\n⚠️  АНАЛИЗ РИСКОВ ДЛЯ СЕРВИСА:\n"
        
        if total_attrition > 15:
            text += "• 🔴 ВЫСОКАЯ ТЕКУЧЕСТЬ: >15% - требуется срочный анализ\n"
        elif total_attrition > 10:
            text += "• 🟠 ПОВЫШЕННАЯ ТЕКУЧЕСТЬ: 10-15% - внимание к retention\n"
        elif total_attrition > 5:
            text += "• 🟡 УМЕРЕННАЯ ТЕКУЧЕСТЬ: 5-10% - в пределах нормы\n"
        else:
            text += "• 🟢 НИЗКАЯ ТЕКУЧЕСТЬ: <5% - отличный показатель\n"
        
        # Демографические особенности
        largest_age_group = age_data.nlargest(1, 'employees')
        if len(largest_age_group) > 0:
            text += f"• 🎯 Преобладающая возрастная группа: {largest_age_group['age_category'].iloc[0]} "
            text += f"({largest_age_group['employees'].iloc[0]/total_employees*100:.1f}%)\n"
        
        return text