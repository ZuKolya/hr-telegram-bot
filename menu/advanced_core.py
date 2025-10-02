# advanced_core.py
import os
import pandas as pd
import logging
import sys
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from menu.data_repository import HRDataRepository
from report_services.company_dynamics_service import CompanyDynamicsService
from report_services.demographic_service import DemographicReportService
from report_services.service_analysis_service import ServiceAnalysisService
from report_services.risk_assessment_service import RiskAssessmentService
from report_services.hiring_recommendations_service import HiringRecommendationsService
from report_services.detailed_service_service import DetailedServiceService
from report_services.service_hiring_service import ServiceHiringService
from config import DB_PATH, PLOT_DIR

EXPERIENCE_ORDER = [
    '1 мес', '2 мес', '3 мес', 'до 1 года', 
    '1-2 года', '2-3 года', '3-5 лет', 'более 5 лет'
]

logger = logging.getLogger(__name__)

class AdvancedHRAnalyzer:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.repo = HRDataRepository(db_path)
        self._initialize_services()
        self.plot_dir = PLOT_DIR
        os.makedirs(self.plot_dir, exist_ok=True)
        self.experience_order = EXPERIENCE_ORDER
    
    def _initialize_services(self):
        self.company_dynamics_service = CompanyDynamicsService(self.repo)
        self.demographic_service = DemographicReportService(self.repo)
        self.service_analysis_service = ServiceAnalysisService(self.repo)
        self.risk_assessment_service = RiskAssessmentService(self.repo)
        self.hiring_recommendations_service = HiringRecommendationsService(self.repo)
        self.detailed_service_service = DetailedServiceService(self.repo)
        self.service_hiring_service = ServiceHiringService(self.repo)
    
    def company_dynamics(self):
        try:
            report_text, plot_data = self.company_dynamics_service.generate_report()
            plot_paths = self._create_dynamics_plots(plot_data) if plot_data is not None else None
            return report_text, plot_paths
        except Exception as e:
            logger.error(f"Ошибка анализа динамики: {str(e)}")
            return f"❌ Ошибка анализа динамики: {str(e)}", None
    
    def demographic_dashboard(self):
        try:
            last_date = self.repo.get_last_report_date()
            report_text, plot_data = self.demographic_service.generate_report(last_date)
            plot_paths = self._create_demographic_plots(plot_data) if plot_data is not None else None
            return report_text, plot_paths
        except Exception as e:
            logger.error(f"Ошибка демографического анализа: {str(e)}")
            return f"❌ Ошибка демографического анализа: {str(e)}", None
    
    def service_analysis(self):
        try:
            last_date = self.repo.get_last_report_date()
            report_text, plot_data = self.service_analysis_service.generate_report(last_date)
            plot_paths = self._create_service_plots(plot_data) if plot_data is not None else None
            return report_text, plot_paths
        except Exception as e:
            logger.error(f"Ошибка анализа сервисов: {str(e)}")
            return f"❌ Ошибка анализа сервисов: {str(e)}", None
    
    def risk_assessment(self):
        try:
            last_date = self.repo.get_last_report_date()
            return self.risk_assessment_service.generate_report(last_date)
        except Exception as e:
            logger.error(f"Ошибка оценки рисков: {str(e)}")
            return f"❌ Ошибка оценки рисков: {str(e)}", None
    
    def hiring_recommendations(self):
        try:
            return self.hiring_recommendations_service.generate_report()
        except Exception as e:
            logger.error(f"Ошибка формирования рекомендаций: {str(e)}")
            return f"❌ Ошибка формирования рекомендаций: {str(e)}", None
    
    def detailed_analysis(self, dimension, value):
        try:
            if dimension.lower() != "service":
                return f"❌ Поддерживается только анализ по сервисам", None
            
            service_mapping = self.repo.get_service_mapping()
            normalized_value = service_mapping.get(value.lower(), value)
            
            date_query = """
            SELECT MAX(report_date) as last_date 
            FROM hr_data_clean 
            WHERE service = ?
            """
            date_result = self.repo.db.execute_query(date_query, (normalized_value,))
            service_last_date = date_result['last_date'].iloc[0]
            
            if service_last_date is None:
                return f"❌ Сервис '{normalized_value}' не найден в базе данных", None
            
            current_last_date = self.repo.get_last_report_date()
            date_note = ""
            if service_last_date != current_last_date:
                date_note = f"\n📅 Примечание: данные актуальны на {service_last_date[:10]} (не на текущую дату)\n"
            
            result, plot_path = self.detailed_service_service.generate_report(normalized_value, service_last_date)
            
            if date_note:
                result = result + date_note
                
            return result, plot_path
            
        except Exception as e:
            logger.error(f"Ошибка детального анализа: {str(e)}")
            return f"❌ Ошибка детального анализа: {str(e)}", None
    
    def hiring_service_analysis(self, service_name):
        try:
            service_mapping = self.repo.get_service_mapping()
            normalized_value = service_mapping.get(service_name.lower(), service_name)
            
            date_query = """
            SELECT MAX(report_date) as last_date 
            FROM hr_data_clean 
            WHERE service = ?
            """
            date_result = self.repo.db.execute_query(date_query, (normalized_value,))
            service_last_date = date_result['last_date'].iloc[0]
            
            if service_last_date is None:
                return f"❌ Сервис '{normalized_value}' не найден в базе данных", None
            
            result, plot_path = self.service_hiring_service.generate_report(normalized_value)
            
            current_last_date = self.repo.get_last_report_date()
            if service_last_date != current_last_date:
                result = result + f"\n📅 Примечание: данные актуальны на {service_last_date[:10]}"
                
            return result, plot_path
            
        except Exception as e:
            logger.error(f"Ошибка анализа найма: {str(e)}")
            return f"❌ Ошибка анализа найма: {str(e)}", None
    
    def get_all_services(self):
        try:
            return self.repo.get_all_services()
        except Exception as e:
            logger.error(f"Ошибка при получении списка сервисов: {e}")
            return ["Такси", "Маркет", "Крауд", "Лавка", "Финтех", "Еда", "Доставка", "Облако"]
    
    def get_service_mapping(self):
        return self.repo.get_service_mapping()

    def _create_demographic_plots(self, plot_data):
        plot_paths = []
        
        try:
            if 'age_data' in plot_data and len(plot_data['age_data']) > 0:
                age_plot = self._create_plot(
                    plot_data['age_data'], 'age_category', 'count',
                    'Распределение по возрастным группам', 'bar',
                    'Возрастная группа', 'Количество сотрудников'
                )
                if age_plot:
                    plot_paths.append(age_plot)
            
            if 'gender_data' in plot_data and len(plot_data['gender_data']) > 0:
                gender_plot = self._create_plot(
                    plot_data['gender_data'], 'sex', 'count',
                    'Гендерное распределение', 'pie'
                )
                if gender_plot:
                    plot_paths.append(gender_plot)
            
            if 'exp_data' in plot_data and len(plot_data['exp_data']) > 0:
                exp_data = plot_data['exp_data'].copy()
                exp_data['order'] = exp_data['experience_category'].apply(
                    lambda x: self.experience_order.index(x) if x in self.experience_order else len(self.experience_order)
                )
                exp_data = exp_data.sort_values('order')
                
                exp_plot = self._create_plot(
                    exp_data, 'experience_category', 'count',
                    'Распределение по опыту работы', 'bar',
                    'Опыт работы', 'Количество сотрудников'
                )
                if exp_plot:
                    plot_paths.append(exp_plot)
                    
        except Exception as e:
            logger.error(f"Ошибка при создании демографических графиков: {e}")
        
        return plot_paths
    
    def _create_dynamics_plots(self, plot_data):
        plot_paths = []
        
        try:
            if plot_data is not None and len(plot_data) > 0:
                dynamics_plot = self._create_plot(
                    plot_data, 'month', 'value',
                    'Динамика наймов и увольнений', 'bar',
                    'Месяц', 'Количество', 'type'
                )
                if dynamics_plot:
                    plot_paths.append(dynamics_plot)
                
        except Exception as e:
            logger.error(f"Ошибка при создании графиков динамики: {e}")
        
        return plot_paths
    
    def _create_service_plots(self, plot_data):
        plot_paths = []
        
        try:
            if plot_data is not None and len(plot_data) > 0:
                large_services = plot_data[plot_data['employees'] > 100]
                
                if len(large_services) > 0:
                    size_plot = self._create_plot(
                        large_services, 'service', 'employees',
                        'Количество сотрудников по сервисам', 'bar',
                        'Сервис', 'Количество сотрудников'
                    )
                    if size_plot:
                        plot_paths.append(size_plot)
                    
                    attrition_plot = self._create_plot(
                        large_services, 'service', 'attrition_rate',
                        'Текучесть кадров по сервисам', 'bar',
                        'Сервис', 'Текучесть (%)'
                    )
                    if attrition_plot:
                        plot_paths.append(attrition_plot)
                    
        except Exception as e:
            logger.error(f"Ошибка при создании графиков сервисов: {e}")
        
        return plot_paths

    def _create_plot(self, data, x_col, y_col, title, plot_type='bar', x_label=None, y_label=None, hue=None):
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
            import numpy as np
            
            plt.figure(figsize=(12, 8))
            colors = sns.color_palette("husl", len(data))
            
            if plot_type == 'bar':
                if hue:
                    unique_categories = data[hue].unique()
                    bar_width = 0.35
                    x_pos = np.arange(len(data[x_col].unique()))
                    
                    for i, category in enumerate(unique_categories):
                        category_data = data[data[hue] == category]
                        bars = plt.bar(x_pos + i * bar_width, category_data[y_col], 
                                      bar_width, label=category, alpha=0.8)
                        plt.bar_label(bars, fmt='%.0f', padding=3)
                    
                    plt.xticks(x_pos + bar_width/2, data[x_col].unique())
                    plt.legend()
                else:
                    bars = plt.bar(data[x_col], data[y_col], color=colors, alpha=0.8)
                    plt.bar_label(bars, fmt='%.1f', padding=3)
                    
            elif plot_type == 'line':
                plt.plot(data[x_col], data[y_col], marker='o', linewidth=3, markersize=8, color=colors[0])
            elif plot_type == 'pie':
                filtered_data = data[data[y_col] > 0]
                if len(filtered_data) > 0:
                    plt.pie(filtered_data[y_col], labels=filtered_data[x_col], autopct='%1.1f%%', 
                           colors=colors, startangle=90)
                plt.axis('equal')
            
            plt.title(title, fontsize=16, fontweight='bold', pad=20)
            if x_label:
                plt.xlabel(x_label, fontsize=12)
            if y_label:
                plt.ylabel(y_label, fontsize=12)
            
            plt.grid(True, alpha=0.3)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            
            from datetime import datetime
            plot_filename = f"{title.replace(' ', '_').replace('/', '_')}_{datetime.now().strftime('%H%M%S')}.png"
            plot_path = os.path.join(self.plot_dir, plot_filename)
            plt.savefig(plot_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"График создан: {plot_path}")
            return plot_path
            
        except Exception as e:
            logger.error(f"Ошибка при создании графика: {e}")
            return None
