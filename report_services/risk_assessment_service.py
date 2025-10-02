# report_services/risk_assessment_service.py
import pandas as pd

class RiskAssessmentService:
    def __init__(self, data_repository):
        self.repo = data_repository
    
    def generate_report(self, report_date):
        """Сгенерировать отчет по оценке рисков"""
        data = self.repo.get_risk_assessment_data(report_date)
        
        if len(data) == 0:
            return "❌ Нет данных для оценки рисков", None
        
        risk_data = data.iloc[0]  # Берем первую строку
        
        # Рассчитываем проценты
        total_employees = risk_data['total_employees']
        zero_fte_percentage = (risk_data['zero_fte_count'] / total_employees) * 100 if total_employees > 0 else 0
        undefined_cluster_percentage = (risk_data['undefined_cluster_count'] / total_employees) * 100 if total_employees > 0 else 0
        undefined_service_percentage = (risk_data['undefined_service_count'] / total_employees) * 100 if total_employees > 0 else 0
        recent_hires_percentage = (risk_data['recent_hires_count'] / total_employees) * 100 if total_employees > 0 else 0
        overall_attrition = (risk_data['total_fires'] / total_employees) * 100 if total_employees > 0 else 0
        
        # Форматируем отчет
        report_text = self._format_report(
            risk_data, total_employees, zero_fte_percentage, 
            undefined_cluster_percentage, undefined_service_percentage, 
            recent_hires_percentage, overall_attrition
        )
        
        return report_text, None
    
    def _format_report(self, data, total_employees, zero_fte_percentage, 
                      undefined_cluster_percentage, undefined_service_percentage,
                      recent_hires_percentage, overall_attrition):
        """Форматирование текста отчета"""
        text = "⚠️  КОМПЛЕКСНАЯ ОЦЕНКА РИСКОВ\n\n"
        
        text += "🎯 КЛЮЧЕВЫЕ МЕТРИКИ БЕЗОПАСНОСТИ:\n"
        text += f"• Общая численность: {int(total_employees):,} чел.\n"
        text += f"• Месячная текучесть: {overall_attrition:.1f}%\n"
        text += f"• Новые сотрудники (<3 мес): {recent_hires_percentage:.1f}%\n\n"
        
        text += f"📊 КАЧЕСТВО ДАННЫХ И РЕСУРСЫ:\n"
        text += f"• Неопределенные кластеры: {undefined_cluster_percentage:.1f}%\n"
        text += f"• Неопределенные сервисы: {undefined_service_percentage:.1f}%\n"
        text += f"• Нулевые ставки (FTE=0): {zero_fte_percentage:.1f}%\n\n"
        
        # Расчет оценки рисков
        risk_score, risk_factors = self._calculate_risk_score(
            overall_attrition, zero_fte_percentage, 
            undefined_cluster_percentage, undefined_service_percentage,
            recent_hires_percentage
        )
        
        text += f"🏆 ОЦЕНКА РИСКОВ ПО ШКАЛЕ 1-10:\n"
        
        if risk_score <= 3:
            risk_status = "🟢 НИЗКИЙ РИСК"
            text += f"• {risk_status} ({risk_score}/10)\n"
        elif risk_score <= 6:
            risk_status = "🟡 СРЕДНИЙ РИСК"
            text += f"• {risk_status} ({risk_score}/10)\n"
        else:
            risk_status = "🔴 ВЫСОКИЙ РИСК"
            text += f"• {risk_status} ({risk_score}/10)\n"
        
        if risk_factors:
            text += f"• Основные факторы риска: {', '.join(risk_factors)}\n"
        
        text += f"\n💡 СТРАТЕГИЧЕСКИЕ РЕКОМЕНДАЦИИ:\n"
        
        # Рекомендации по нулевым ставкам
        if zero_fte_percentage > 20:
            text += "• 🔴 СРОЧНО: Аудит системы учета рабочего времени\n"
            text += "• Внедрение контроля за заполнением ставок\n\n"
        elif zero_fte_percentage > 10:
            text += "• 🟠 ВЫСОКИЙ ПРИОРИТЕТ: Проверка данных FTE\n"
            text += "• Обучение ответственных за ввод данных\n\n"
        elif zero_fte_percentage > 5:
            text += "• 🟡 СРЕДНИЙ ПРИОРИТЕТ: Мониторинг заполнения ставок\n"
        
        # Рекомендации по неопределенным кластерам
        if undefined_cluster_percentage > 50:
            text += "• 🔴 СРОЧНО: Ревизия системы классификации должностей\n"
            text += "• Обучение HR-отдела правильному заполнению данных\n\n"
        elif undefined_cluster_percentage > 30:
            text += "• 🟠 ВЫСОКИЙ ПРИОРИТЕТ: Уточнение классификатора кластеров\n"
            text += "• Автоматизация проверки заполнения полей\n\n"
        elif undefined_cluster_percentage > 15:
            text += "• 🟡 СРЕДНИЙ ПРИОРИТЕТ: Улучшение качества данных кластеров\n"
        
        # Рекомендации по неопределенным сервисам
        if undefined_service_percentage > 10:
            text += "• 🔴 СРОЧНО: Аудит привязки сотрудников к сервисам\n"
            text += "• Внедрение обязательного заполнения сервиса\n\n"
        elif undefined_service_percentage > 5:
            text += "• 🟠 ВЫСОКИЙ ПРИОРИТЕТ: Проверка данных по сервисам\n"
            text += "• Уточнение матрицы подчиненности\n\n"
        elif undefined_service_percentage > 2:
            text += "• 🟡 СРЕДНИЙ ПРИОРИТЕТ: Мониторинг заполнения сервисов\n"
        
        # Рекомендации по текучести
        if overall_attrition > 10:
            text += "• 🔴 ВЫСОКИЙ РИСК: Глубокий анализ причин увольнений\n"
            text += "• Внедрение exit-интервью и программ удержания\n\n"
        elif overall_attrition > 5:
            text += "• 🟠 СРЕДНИЙ РИСК: Мониторинг показателей увольнений\n"
            text += "• Улучшение программ адаптации\n\n"
        
        # Рекомендации по новичкам
        if recent_hires_percentage > 20:
            text += "• 🔴 ВЫСОКИЙ РИСК: Усиление программы адаптации\n"
            text += "• Мониторинг успешности probation period\n\n"
        elif recent_hires_percentage > 10:
            text += "• 🟠 СРЕДНИЙ РИСК: Оптимизация процессов онбординга\n"
        
        text += f"\n📈 ДОПОЛНИТЕЛЬНО:\n"
        text += f"• Рекомендуется ежемесячный мониторинг этих метрик\n"
        text += f"• Внедрение системы раннего предупреждения рисков\n"
        
        return text
    
    def _calculate_risk_score(self, attrition, zero_fte, undefined_cluster, undefined_service, recent_hires):
        """Расчет оценки рисков"""
        risk_score = 0
        risk_factors = []
        
        # Оценка текучести
        if attrition > 10:
            risk_score += 3
            risk_factors.append("высокая текучесть")
        elif attrition > 5:
            risk_score += 1
            risk_factors.append("умеренная текучесть")
        
        # Оценка нулевых ставок
        if zero_fte > 20:
            risk_score += 3
            risk_factors.append("критический уровень нулевых ставок")
        elif zero_fte > 10:
            risk_score += 2
            risk_factors.append("высокий уровень нулевых ставок")
        elif zero_fte > 5:
            risk_score += 1
            risk_factors.append("повышенный уровень нулевых ставок")
        
        # Оценка неопределенных кластеров
        if undefined_cluster > 50:
            risk_score += 3
            risk_factors.append("критическое качество данных кластеров")
        elif undefined_cluster > 30:
            risk_score += 2
            risk_factors.append("низкое качество данных кластеров")
        elif undefined_cluster > 15:
            risk_score += 1
            risk_factors.append("умеренное качество данных кластеров")
        
        # Оценка неопределенных сервисов
        if undefined_service > 10:
            risk_score += 2
            risk_factors.append("высокий уровень неопределенных сервисов")
        elif undefined_service > 5:
            risk_score += 1
            risk_factors.append("повышенный уровень неопределенных сервисов")
        
        # Оценка новичков
        if recent_hires > 20:
            risk_score += 2
            risk_factors.append("высокая доля новичков")
        elif recent_hires > 10:
            risk_score += 1
            risk_factors.append("повышенная доля новичков")
        
        risk_score = min(risk_score, 10)
        return risk_score, risk_factors