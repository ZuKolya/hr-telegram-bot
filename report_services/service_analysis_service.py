# report_services/service_analysis_service.py
import pandas as pd

class ServiceAnalysisService:
    def __init__(self, data_repository):
        self.repo = data_repository
    
    def generate_report(self, report_date):
        """Сгенерировать анализ по сервисам"""
        data = self.repo.get_service_stats(report_date)
        
        if len(data) == 0:
            return "❌ Нет данных по сервисам", None
        
        # Рассчитываем дополнительные метрики
        total_employees = data['employees'].sum()
        data['attrition_rate'] = (data['fires'] / data['employees'].replace(0, 1)) * 100
        data['attrition_rate'] = data['attrition_rate'].fillna(0)
        data['percentage'] = (data['employees'] / total_employees) * 100
        
        # Форматируем отчет
        report_text = self._format_report(data, total_employees, report_date)
        
        return report_text, data
    
    def _format_report(self, data, total_employees, report_date):
        """Форматирование текста отчета"""
        text = f"🏢 АНАЛИЗ ПО СЕРВИСАМ на {report_date[:10]}\n\n"
        
        text += f"📊 Общая численность: {total_employees:,} чел.\n\n"
        
        text += "👥 Количество сотрудников по сервисам:\n"
        for _, row in data.iterrows():
            text += f"• {row['service']}: {int(row['employees']):,} чел. ({row['percentage']:.1f}%)\n"
        
        text += "\n📈 Текучесть кадров:\n"
        for _, row in data.iterrows():
            if pd.isna(row['attrition_rate']) or row['attrition_rate'] == 0:
                status = "➖"
                rate = "0.0%"
            else:
                status = "✅" if row['attrition_rate'] < 5 else "⚠️" if row['attrition_rate'] < 10 else "❌"
                rate = f"{row['attrition_rate']:.1f}%"
            text += f"• {row['service']}: {status} {rate}\n"
        
        # Анализ распределения
        text += f"\n📊 РАСПРЕДЕЛЕНИЕ ПО СЕРВИСАМ:\n"
        
        # Топ-5 сервисов по размеру
        top_5 = data.nlargest(5, 'employees')
        text += f"• Топ-5 сервисов: {', '.join(top_5['service'].tolist())}\n"
        text += f"• Доля топ-5: {top_5['percentage'].sum():.1f}% от общей численности\n"
        
        # Анализ текучести
        best_service = data[data['attrition_rate'] > 0].nsmallest(1, 'attrition_rate')
        worst_service = data.nlargest(1, 'attrition_rate')
        
        if len(best_service) > 0 and best_service['attrition_rate'].iloc[0] > 0:
            text += f"• 🏆 Лучший показатель: {best_service['service'].iloc[0]} ({best_service['attrition_rate'].iloc[0]:.1f}% текучести)\n"
        
        if len(worst_service) > 0 and worst_service['attrition_rate'].iloc[0] > 5:
            text += f"• ⚠️  Высокая текучесть: {worst_service['service'].iloc[0]} ({worst_service['attrition_rate'].iloc[0]:.1f}%)\n"
        
        # Концентрация персонала
        largest_service = data.nlargest(1, 'employees')
        if len(largest_service) > 0:
            concentration = largest_service['percentage'].iloc[0]
            text += f"• 🎯 Крупнейший сервис: {largest_service['service'].iloc[0]} ({concentration:.1f}% персонала)\n"
            
            if concentration > 30:
                text += "  • 💡 Высокая концентрация - риск зависимост\n"
        
        return text