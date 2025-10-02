# data_repository.py
import pandas as pd
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import DatabaseManager

logger = logging.getLogger(__name__)

class HRDataRepository:
    def __init__(self, db_path):
        self.db = DatabaseManager(db_path)
    
    def get_last_report_date(self):
        query = "SELECT MAX(report_date) as last_date FROM hr_data_clean"
        result = self.db.execute_query(query)
        return result['last_date'].iloc[0]
    
    def get_employee_id(self, row):
        return (
            f"{row['hire_to_company']}_"
            f"{row['sex']}_"
            f"{row['fullyears']}_"
            f"{row['location_name']}_"
            f"{row['department_3']}_"
            f"{row['department_4']}_"
            f"{row['department_5']}_"
            f"{row['department_6']}"
        )
    
    def _get_employee_id_expression(self):
        return (
            "hire_to_company || '_' || sex || '_' || fullyears || '_' || "
            "location_name || '_' || department_3 || '_' || "
            "department_4 || '_' || department_5 || '_' || department_6"
        )
    
    def get_demographics_data(self, report_date):
        query = f"""
        SELECT 
            age_category,
            sex,
            experience_category,
            COUNT(DISTINCT {self._get_employee_id_expression()}) as count
        FROM hr_data_clean 
        WHERE report_date = ?
        GROUP BY age_category, sex, experience_category
        """
        return self.db.execute_query(query, (report_date,))
    
    def get_company_dynamics(self):
        query = f"""
        SELECT 
            strftime('%Y-%m', report_date) as month,
            SUM(hirecount) as hires,
            SUM(firecount) as fires,
            COUNT(DISTINCT {self._get_employee_id_expression()}) as total_employees
        FROM hr_data_clean 
        GROUP BY strftime('%Y-%m', report_date)
        ORDER BY month
        """
        return self.db.execute_query(query)
    
    def get_service_stats(self, report_date):
        query = f"""
        SELECT 
            service,
            COUNT(DISTINCT {self._get_employee_id_expression()}) as employees,
            SUM(hirecount) as hires,
            SUM(firecount) as fires
        FROM hr_data_clean 
        WHERE report_date = ?
        GROUP BY service
        ORDER BY employees DESC
        """
        return self.db.execute_query(query, (report_date,))
    
    def get_risk_assessment_data(self, report_date):
        query = f"""
        SELECT 
            COUNT(DISTINCT {self._get_employee_id_expression()}) as total_employees,
            SUM(hirecount) as total_hires,
            SUM(firecount) as total_fires,
            SUM(CASE WHEN fte = 0 THEN 1 ELSE 0 END) as zero_fte_count,
            SUM(CASE WHEN cluster = 'Не определен Кластер' OR cluster = 'Другое' THEN 1 ELSE 0 END) as undefined_cluster_count,
            SUM(CASE WHEN service = 'Не определен Сервис' THEN 1 ELSE 0 END) as undefined_service_count,
            SUM(CASE WHEN experience < 3 THEN 1 ELSE 0 END) as recent_hires_count
        FROM hr_data_clean 
        WHERE report_date = ?
        """
        return self.db.execute_query(query, (report_date,))
    
    def get_hiring_recommendations_data(self):
        """Получить данные для рекомендаций по найму"""
        try:
            # Берем данные за июль и август 2025 года
            query = f"""
            SELECT 
                service,
                SUM(firecount) as monthly_fires,
                COUNT(DISTINCT {self._get_employee_id_expression()}) as total_employees
            FROM hr_data_clean 
            WHERE report_date IN ('2025-07-31', '2025-08-31')
            GROUP BY service
            HAVING total_employees > 0  -- Только сервисы с сотрудниками
            """
            data = self.db.execute_query(query)
            
            # Если данных нет, пробуем альтернативный формат дат (с временем)
            if len(data) == 0:
                query_alt = f"""
                SELECT 
                    service,
                    SUM(firecount) as monthly_fires,
                    COUNT(DISTINCT {self._get_employee_id_expression()}) as total_employees
                FROM hr_data_clean 
                WHERE report_date LIKE '2025-07-31%' OR report_date LIKE '2025-08-31%'
                GROUP BY service
                HAVING total_employees > 0
                """
                data = self.db.execute_query(query_alt)
            
            logger.info(f"Данные для рекомендаций по найму: {len(data)} строк")
            if len(data) > 0:
                logger.info(f"Найдены сервисы: {data['service'].tolist()}")
            
            return data
            
        except Exception as e:
            logger.error(f"Ошибка получения данных для рекомендаций по найму: {e}")
            return pd.DataFrame()
    
    def get_detailed_service_analysis(self, service_name, report_date):
        """Получить детальный анализ сервиса"""
        try:
            query = f"""
            SELECT 
                age_category,
                sex,
                experience_category,
                COUNT(DISTINCT {self._get_employee_id_expression()}) as employees,
                SUM(firecount) as fires,
                AVG(fullyears) as avg_age,
                AVG(experience) as avg_experience
            FROM hr_data_clean 
            WHERE service = ? AND report_date = ?
            GROUP BY age_category, sex, experience_category
            ORDER BY employees DESC
            """
            return self.db.execute_query(query, (service_name, report_date))
        except Exception as e:
            logger.error(f"Ошибка получения детального анализа для сервиса {service_name}: {e}")
            return pd.DataFrame()
    
    def get_service_hiring_analysis(self, service_name):
        """Получить анализ найма для конкретного сервиса"""
        try:
            query_basic = f"""
            SELECT 
                COUNT(DISTINCT {self._get_employee_id_expression()}) as total_employees,
                SUM(firecount) as total_fires,
                AVG(fullyears) as avg_age,
                AVG(experience) as avg_experience
            FROM hr_data_clean 
            WHERE service = ? AND report_date = '2025-08-31'
            """
            
            query_hiring = f"""
            SELECT 
                SUM(firecount) as monthly_fires
            FROM hr_data_clean 
            WHERE service = ? 
            AND report_date IN ('2025-07-31', '2025-08-31')
            """
            
            basic_stats = self.db.execute_query(query_basic, (service_name,))
            hiring_stats = self.db.execute_query(query_hiring, (service_name,))
            
            # Если нет данных с точными датами, пробуем с LIKE
            if len(basic_stats) == 0:
                query_basic_alt = f"""
                SELECT 
                    COUNT(DISTINCT {self._get_employee_id_expression()}) as total_employees,
                    SUM(firecount) as total_fires,
                    AVG(fullyears) as avg_age,
                    AVG(experience) as avg_experience
                FROM hr_data_clean 
                WHERE service = ? AND report_date LIKE '2025-08-31%'
                """
                basic_stats = self.db.execute_query(query_basic_alt, (service_name,))
            
            if len(hiring_stats) == 0:
                query_hiring_alt = f"""
                SELECT 
                    SUM(firecount) as monthly_fires
                FROM hr_data_clean 
                WHERE service = ? 
                AND (report_date LIKE '2025-07-31%' OR report_date LIKE '2025-08-31%')
                """
                hiring_stats = self.db.execute_query(query_hiring_alt, (service_name,))
            
            logger.info(f"Анализ найма для сервиса {service_name}: basic_stats={len(basic_stats)}, hiring_stats={len(hiring_stats)}")
            
            return {
                'basic_stats': basic_stats,
                'hiring_stats': hiring_stats
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения анализа найма для сервиса {service_name}: {e}")
            return {'basic_stats': pd.DataFrame(), 'hiring_stats': pd.DataFrame()}
    
    def get_all_services(self):
        query = """
        SELECT DISTINCT service 
        FROM hr_data_clean 
        WHERE service IS NOT NULL AND service != '' AND service != 'Не определен Сервис'
        ORDER BY service
        """
        result = self.db.execute_query(query)
        return result['service'].tolist()
    
    def get_service_mapping(self):
        return {
            'такси': 'Такси',
            'маркет': 'Маркет', 
            'крауд': 'Крауд',
            'лавка': 'Лавка',
            'финтех': 'Финтех',
            'еда': 'Еда',
            'доставка': 'Доставка',
            'беспилотники': 'Беспилотники',
            'беспилотные автомобили': 'Беспилотные автомобили',
            'вертикали': 'Вертикали',
            'коммерческий департамент': 'Коммерческий департамент',
            'облако': 'Облако',
            'образовательные инициативы': 'Образовательные инициативы',
            'общие подразделения': 'Общие подразделения',
            'плюс и фантех': 'Плюс и Фантех',
            'поисковый портал': 'Поисковый портал',
            'не определен сервис': 'Не определен Сервис'
        }
    
    def find_service_by_alias(self, service_name):
        service_mapping = self.get_service_mapping()
        
        if service_name in service_mapping.values():
            return service_name
        
        normalized_name = service_name.lower()
        if normalized_name in service_mapping:
            return service_mapping[normalized_name]
        
        query = f"""
        SELECT DISTINCT service 
        FROM hr_data_clean 
        WHERE service LIKE '%{service_name}%' 
        OR service LIKE '%{service_name.capitalize()}%'
        LIMIT 1
        """
        
        try:
            result = self.db.execute_query(query)
            if len(result) > 0:
                return result['service'].iloc[0]
        except:
            pass
        
        return None

    def debug_service_data(self, service_name):
        """Диагностика данных по сервису"""
        try:
            # Проверим, какие сервисы вообще есть в базе
            all_services_query = "SELECT DISTINCT service FROM hr_data_clean ORDER BY service"
            all_services = self.db.execute_query(all_services_query)
            logger.info(f"Все сервисы в базе: {all_services['service'].tolist()}")
            
            # Проверим данные по конкретному сервису
            service_data_query = f"""
            SELECT service, report_date, COUNT(*) as count 
            FROM hr_data_clean 
            WHERE service = ? 
            GROUP BY service, report_date
            ORDER BY report_date DESC
            LIMIT 5
            """
            service_data = self.db.execute_query(service_data_query, (service_name,))
            logger.info(f"Данные для сервиса '{service_name}': {service_data.to_dict('records')}")
            
            return service_data
        except Exception as e:
            logger.error(f"Ошибка диагностики сервиса {service_name}: {e}")
            return pd.DataFrame()