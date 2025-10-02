# report_services/demographic_service.py
import pandas as pd

class DemographicReportService:
    def __init__(self, data_repository):
        self.repo = data_repository
    
    def generate_report(self, report_date):
        """Сгенерировать демографический отчет"""
        data = self.repo.get_demographics_data(report_date)
        
        if len(data) == 0:
            return "❌ Нет демографических данных", None
        
        # Группируем данные
        age_data = data.groupby('age_category', as_index=False)['count'].sum()
        gender_data = data.groupby('sex', as_index=False)['count'].sum()
        exp_data = data.groupby('experience_category', as_index=False)['count'].sum()
        
        # Форматируем отчет
        report_text = self._format_report(age_data, gender_data, exp_data, report_date)
        
        # Подготавливаем данные для графиков
        plot_data = {
            'age_data': age_data,
            'gender_data': gender_data,
            'exp_data': exp_data,
            'report_date': report_date
        }
        
        return report_text, plot_data
    
    def _format_report(self, age_data, gender_data, exp_data, report_date):
        """Форматирование текста отчета"""
        total_employees = age_data['count'].sum()
        
        text = f"👥 ДЕМОГРАФИЧЕСКИЙ АНАЛИЗ на {report_date[:10]}\n\n"
        text += f"📊 Общая численность: {total_employees:,} чел.\n\n"
        
        text += "📊 Возрастные группы:\n"
        age_data = age_data.sort_values('count', ascending=False)
        for _, row in age_data.iterrows():
            percentage = (row['count'] / total_employees) * 100
            text += f"• {row['age_category']}: {percentage:.1f}% ({int(row['count']):,} чел.)\n"
        
        text += "\n👫 Гендерный состав:\n"
        gender_data = gender_data.sort_values('count', ascending=False)
        for _, row in gender_data.iterrows():
            percentage = (row['count'] / total_employees) * 100
            gender_name = "Женщины" if row['sex'] == 'F' else "Мужчины"
            text += f"• {gender_name}: {percentage:.1f}% ({int(row['count']):,} чел.)\n"
        
        text += "\n📅 Опыт работы:\n"
        # Правильный порядок сортировки для опыта
        exp_order = ['1 мес', '2 мес', '3 мес', 'до 1 года', '1-2 года', '2-3 года', '3-5 лет', 'более 5 лет']
        
        # Создаем временную колонку для сортировки
        exp_data['order'] = exp_data['experience_category'].apply(
            lambda x: exp_order.index(x) if x in exp_order else len(exp_order)
        )
        exp_data = exp_data.sort_values('order')
        
        for _, row in exp_data.iterrows():
            percentage = (row['count'] / total_employees) * 100
            text += f"• {row['experience_category']}: {percentage:.1f}% ({int(row['count']):,} чел.)\n"
        
        # Анализ демографических трендов
        text += f"\n📈 ДЕМОГРАФИЧЕСКИЕ ТРЕНДЫ:\n"
        
        # Самые крупные возрастные группы
        top_age = age_data.nlargest(2, 'count')
        text += f"• Преобладают сотрудники: {top_age['age_category'].iloc[0]} ({top_age['count'].iloc[0]/total_employees*100:.1f}%)"
        if len(top_age) > 1:
            text += f" и {top_age['age_category'].iloc[1]} ({top_age['count'].iloc[1]/total_employees*100:.1f}%)\n"
        
        # Гендерный баланс
        if len(gender_data) == 2:
            ratio = max(gender_data['count'].iloc[0], gender_data['count'].iloc[1]) / min(gender_data['count'].iloc[0], gender_data['count'].iloc[1])
            if ratio > 2:
                text += "• Заметный гендерный дисбаланс\n"
            else:
                text += "• Сбалансированный гендерный состав\n"
        
        return text