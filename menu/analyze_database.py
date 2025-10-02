import sqlite3
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_PATH

def analyze_database():
    """Анализ структуры и содержания базы данных"""
    
    try:
        # Подключаемся к базе
        conn = sqlite3.connect(DB_PATH)
        
        print("🔍 АНАЛИЗ БАЗЫ ДАННЫХ HR_BOT\n")
        
        # 1. Основная информация о таблице
        print("1. 📋 СТРУКТУРА ТАБЛИЦЫ:")
        table_info = pd.read_sql_query("PRAGMA table_info(hr_data_clean)", conn)
        print(table_info.to_string(index=False))
        print()
        
        # 2. Основная статистика
        print("2. 📊 ОСНОВНАЯ СТАТИСТИКА:")
        basic_stats = pd.read_sql_query("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT report_date) as unique_dates,
                MIN(report_date) as first_date,
                MAX(report_date) as last_date
            FROM hr_data_clean
        """, conn)
        print(basic_stats.to_string(index=False))
        print()
        
        # 3. Уникальные значения ключевых колонок (ИСПРАВЛЕННЫЙ ЗАПРОС)
        print("3. 🏷️ УНИКАЛЬНЫЕ ЗНАЧЕНИЯ:")
        
        columns_to_analyze = ['service', 'location_name', 'cluster', 'sex', 
                             'department_3', 'age_category', 'experience_category']
        
        for column in columns_to_analyze:
            # Сначала получаем количество уникальных значений
            count_result = pd.read_sql_query(f"""
                SELECT COUNT(DISTINCT {column}) as unique_count
                FROM hr_data_clean
                WHERE {column} IS NOT NULL
            """, conn)
            
            # Затем получаем примеры значений (первые 10)
            sample_result = pd.read_sql_query(f"""
                SELECT DISTINCT {column}
                FROM hr_data_clean
                WHERE {column} IS NOT NULL
                LIMIT 10
            """, conn)
            
            unique_count = count_result['unique_count'].iloc[0]
            sample_values = sample_result[column].tolist()
            
            print(f"{column}: {unique_count} уникальных значений")
            print(f"   Примеры: {', '.join(map(str, sample_values[:5]))}")
            if unique_count > 5:
                print(f"   ... и еще {unique_count - 5} других")
            print()
        
        # 4. Статистика по числовым колонкам
        print("4. 🔢 ЧИСЛОВЫЕ ДАННЫЕ:")
        numeric_stats = pd.read_sql_query("""
            SELECT 
                'fullyears' as column_name,
                COUNT(fullyears) as non_null_count,
                ROUND(AVG(fullyears), 2) as avg_value,
                MIN(fullyears) as min_value,
                MAX(fullyears) as max_value
            FROM hr_data_clean
            UNION ALL
            SELECT 
                'experience',
                COUNT(experience),
                ROUND(AVG(experience), 2),
                MIN(experience),
                MAX(experience)
            FROM hr_data_clean
            UNION ALL
            SELECT 
                'fte',
                COUNT(fte),
                ROUND(AVG(fte), 2),
                MIN(fte),
                MAX(fte)
            FROM hr_data_clean
            UNION ALL
            SELECT 
                'real_day',
                COUNT(real_day),
                ROUND(AVG(real_day), 2),
                MIN(real_day),
                MAX(real_day)
            FROM hr_data_clean
        """, conn)
        print(numeric_stats.to_string(index=False))
        print()
        
        # 5. Анализ дат и флагов
        print("5. 📅 ДАТЫ И ФЛАГИ:")
        date_analysis = pd.read_sql_query("""
            SELECT 
                report_date,
                SUM(hirecount) as total_hires,
                SUM(firecount) as total_fires,
                COUNT(*) as total_employees,
                ROUND(CAST(SUM(firecount) AS FLOAT) / COUNT(*) * 100, 2) as turnover_rate_percent
            FROM hr_data_clean
            GROUP BY report_date
            ORDER BY report_date
        """, conn)
        print(date_analysis.to_string(index=False))
        print()
        
        # 6. Распределение по сервисам (последняя дата)
        print("6. 🏢 РАСПРЕДЕЛЕНИЕ ПО СЕРВИСАМ (последняя дата):")
        service_stats = pd.read_sql_query("""
            SELECT 
                service,
                COUNT(*) as employee_count,
                ROUND(AVG(fullyears), 2) as avg_age,
                ROUND(AVG(experience), 2) as avg_experience_months,
                ROUND(AVG(fte), 2) as avg_fte,
                SUM(CASE WHEN sex = 'M' THEN 1 ELSE 0 END) as male_count,
                SUM(CASE WHEN sex = 'F' THEN 1 ELSE 0 END) as female_count
            FROM hr_data_clean
            WHERE report_date = (SELECT MAX(report_date) FROM hr_data_clean)
            GROUP BY service
            ORDER BY employee_count DESC
        """, conn)
        print(service_stats.to_string(index=False))
        print()
        
        # 7. Примеры записей
        print("7. 📝 ПРИМЕРЫ ЗАПИСЕЙ (первые 10):")
        samples = pd.read_sql_query("""
            SELECT 
                service, location_name, cluster, sex, fullyears as age,
                experience, fte, hire_to_company, fire_from_company,
                hirecount, firecount, report_date
            FROM hr_data_clean
            WHERE report_date = (SELECT MAX(report_date) FROM hr_data_clean)
            ORDER BY service, location_name
            LIMIT 10
        """, conn)
        print(samples.to_string(index=False))
        print()
        
        # 8. Проверка качества данных
        print("8. ✅ ПРОВЕРКА КАЧЕСТВА ДАННЫХ:")
        quality_check = pd.read_sql_query("""
            SELECT 
                'Работающие с firecount=1' as check_type,
                COUNT(*) as count
            FROM hr_data_clean
            WHERE firecount = 1 AND fire_from_company = '1970-01-01 00:00:00'
            UNION ALL
            SELECT 
                'Уволенные с firecount=0',
                COUNT(*)
            FROM hr_data_clean
            WHERE firecount = 0 AND fire_from_company != '1970-01-01 00:00:00'
            UNION ALL
            SELECT 
                'Записи без сервиса',
                COUNT(*)
            FROM hr_data_clean
            WHERE service IS NULL OR service = ''
            UNION ALL
            SELECT 
                'Записи без локации',
                COUNT(*)
            FROM hr_data_clean
            WHERE location_name IS NULL OR location_name = ''
        """, conn)
        print(quality_check.to_string(index=False))
        print()
        
        # 9. Дополнительная проверка - какие сервисы существуют
        print("9. 🎯 ВСЕ СЕРВИСЫ В БАЗЕ:")
        all_services = pd.read_sql_query("""
            SELECT DISTINCT service
            FROM hr_data_clean
            WHERE service IS NOT NULL
            ORDER BY service
        """, conn)
        print(all_services.to_string(index=False))
        print()
        
        # 10. Проверка значений ставок (FTE)
        print("10. 💰 РАСПРЕДЕЛЕНИЕ СТАВОК (FTE):")
        fte_distribution = pd.read_sql_query("""
            SELECT 
                fte,
                COUNT(*) as employee_count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM hr_data_clean WHERE report_date = (SELECT MAX(report_date) FROM hr_data_clean)), 2) as percentage
            FROM hr_data_clean
            WHERE report_date = (SELECT MAX(report_date) FROM hr_data_clean)
            GROUP BY fte
            ORDER BY fte
        """, conn)
        print(fte_distribution.to_string(index=False))
        
        conn.close()
        
        print("\n✅ Анализ завершен!")
        
    except Exception as e:
        print(f"❌ Ошибка при анализе базы данных: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_database()