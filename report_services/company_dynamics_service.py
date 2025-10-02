# report_services/company_dynamics_service.py
import pandas as pd
from datetime import datetime

class CompanyDynamicsService:
    def __init__(self, data_repository):
        self.repo = data_repository
    
    def generate_report(self):
        """Сгенерировать отчет по динамике компании"""
        dynamics_data = self.repo.get_company_dynamics()
        
        if len(dynamics_data) == 0:
            return "❌ Нет данных для анализа динамики", None
        
        # Форматируем данные
        report_text = self._format_report_text(dynamics_data)
        plot_data = self._prepare_plot_data(dynamics_data)
        
        return report_text, plot_data
    
    def _format_report_text(self, data):
        """Форматирование текстового отчета"""
        text = "📈 ДИНАМИКА КОМПАНИИ\n\n"
        
        # Добавляем детальную статистику по месяцам
        text += "📊 ДЕТАЛЬНАЯ СТАТИСТИКА ПО МЕСЯЦАМ:\n"
        month_names = {'2025-07': 'Июль', '2025-08': 'Август', '2025-09': 'Сентябрь'}
        
        for _, row in data.iterrows():
            month_name = month_names.get(row['month'], row['month'])
            net_growth = row['hires'] - row['fires']
            trend = "▲" if net_growth > 0 else "▼"
            
            text += f"• {month_name}:\n"
            text += f"  Сотрудников: {int(row['total_employees']):,} чел.\n"
            text += f"  Наймы: {int(row['hires'])} чел.\n"
            text += f"  Увольнения: {int(row['fires'])} чел.\n"
            text += f"  Чистый прирост: {trend} {abs(int(net_growth))} чел.\n\n"
        
        # Добавляем примечание про сентябрь
        text += "📝 Примечание: Данные за сентябрь актуальны на 3-е число месяца, \n"
        text += "в то время как данные за июль и август - на конец месяца. \n"
        text += "Это объясняет разницу в абсолютных показателях.\n\n"
        
        # Итоги за период
        total_hires = data['hires'].sum()
        total_fires = data['fires'].sum()
        total_growth = total_hires - total_fires
        avg_employees = data['total_employees'].mean()
        
        text += f"📋 ИТОГИ ЗА ПЕРИОД:\n"
        text += f"• Средняя численность: {avg_employees:,.0f} чел.\n"
        text += f"• Всего наймов: {int(total_hires):,} чел.\n"
        text += f"• Всего увольнений: {int(total_fires):,} чел.\n"
        text += f"• Чистый прирост: {total_growth:+,.0f} чел.\n"
        text += f"• Общая текучесть: {(total_fires/avg_employees)*100:.1f}%\n\n"
        
        # Анализ трендов
        if total_growth > 0:
            growth_percentage = (total_growth / avg_employees) * 100
            text += f"• 📈 КОМПАНИЯ РАСТЕТ (+{growth_percentage:.1f}% за период)\n"
        else:
            text += f"• 📉 СОКРАЩЕНИЕ ЧИСЛЕННОСТИ\n"
        
        # Анализ последних трендов
        if len(data) >= 2:
            last_growth = data['hires'].iloc[-1] - data['fires'].iloc[-1]
            prev_growth = data['hires'].iloc[-2] - data['fires'].iloc[-2]
            
            text += f"\n📈 АНАЛИЗ ТРЕНДОВ:\n"
            if last_growth > prev_growth:
                text += "• Ускоряется рост численности\n"
            elif last_growth < prev_growth:
                text += "• Замедляется рост/ускоряется сокращение\n"
            else:
                text += "• ↔️  Стабильная динамика\n"
        
        return text
    
    def _prepare_plot_data(self, data):
        """Подготовка данных для графика"""
        if len(data) == 0:
            return None
            
        # Создаем данные для группированного графика
        plot_data = pd.DataFrame()
        
        for _, row in data.iterrows():
            hires_row = {'month': row['month'], 'value': row['hires'], 'type': 'Наймы'}
            fires_row = {'month': row['month'], 'value': row['fires'], 'type': 'Увольнения'}
            plot_data = pd.concat([plot_data, pd.DataFrame([hires_row, fires_row])], ignore_index=True)
        
        return plot_data