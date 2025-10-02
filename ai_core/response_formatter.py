#response_formatter.py:
import pandas as pd
from typing import Optional
from .constants import METRIC_NAMES

class ResponseFormatter:
    @staticmethod
    def format_numeric_statistics(result: pd.DataFrame, column_name: str, user_query: str = "") -> str:
        if len(result) == 0 or result['count'].iloc[0] == 0:
            return f"❌ Нет данных для анализа"
        
        row = result.iloc[0]
        
        query_lower = user_query.lower()
        
        if 'мужчин' in query_lower and 'ставк' in query_lower:
            return f"👨 Мужчин на полной ставке: {row['count']:,.0f}".replace(',', ' ')
        
        if any(word in query_lower for word in ['удален', 'дистанц', 'remote']):
            service_match = re.search(r'в (\w+)', query_lower)
            if service_match:
                service = service_match.group(1)
                return f"🏠 Удаленных сотрудников в {service.capitalize()}: {row['count']:,.0f}".replace(',', ' ')
            return f"🏠 Удаленных сотрудников: {row['count']:,.0f}".replace(',', ' ')
        
        if '0.5' in user_query or 'ставк' in query_lower or 'fte' in query_lower:
            return f"Сотрудников на 0.5 ставки: {row['count']:,.0f}".replace(',', ' ')
        
        if 'ставк' in query_lower:
            return f"Статистика по ставкам:\n• Сотрудников: {row['count']:,.0f}\n• Средняя ставка: {row['average']:.2f}".replace(',', ' ')
        
        if 'опыт' in query_lower and 'средн' not in query_lower:
            return f"Сотрудников с указанным опытом: {row['count']:,.0f}".replace(',', ' ')
        
        if 'возраст' in query_lower and 'средн' not in query_lower:
            return f"Сотрудников в указанном возрасте: {row['count']:,.0f}".replace(',', ' ')
        
        return (f"📊 Статистика:\n"
               f"• Количество записей: {row['count']:,.0f}\n"
               f"• Среднее значение: {row['average']:.2f}\n"
               f"• Минимальное значение: {row['min']}\n"
               f"• Максимальное значение: {row['max']}\n"
               f"• Сумма: {row['sum']:,.1f}").replace(',', ' ')

    @staticmethod
    def format_categorical_statistics(result: pd.DataFrame, column_name: str) -> str:
        if len(result) == 0:
            return f"❌ Нет данных для анализа"
        
        total = result['count'].sum()
        
        column_names = {
            'location_name': 'локациям',
            'experience_category': 'категориям опыта',
            'age_category': 'возрастным категориям',
            'cluster': 'кластерам',
            'service': 'сервисам',
            'sex': 'полу',
            'department_3': 'отделам 3 уровня',
            'department_4': 'отделам 4 уровня',
            'fte': 'ставкам'
        }
        
        display_name = column_names.get(column_name, column_name)
        
        response = f"📊 Распределение по {display_name}:\n\n"
        
        for _, row in result.iterrows():
            percentage = (row['count'] / total) * 100
            value = row[column_name]
            
            if column_name == 'fte':
                fte_names = {
                    0.0: "0.0 (нулевая)",
                    0.2: "0.2", 
                    0.37: "0.37",
                    0.4: "0.4",
                    0.5: "0.5 (половинная)",
                    0.75: "0.75",
                    1.0: "1.0 (полная)"
                }
                value = fte_names.get(value, f"{value}")
            
            if column_name == 'sex':
                value = 'Женщины' if value == 'F' else 'Мужчины' if value == 'M' else value
            
            response += f"• {value}: {row['count']:,.0f} ({percentage:.1f}%)\n".replace(',', ' ')
        
        return response

    @staticmethod
    def format_cross_analysis(result: pd.DataFrame, column_x: str, column_y: str) -> str:
        if len(result) == 0:
            return "❌ Нет данных для перекрестного анализа"
        
        response = f"🔀 Перекрестный анализ {column_x} × {column_y}:\n\n"
        current_x = None
        
        for _, row in result.iterrows():
            if row[column_x] != current_x:
                if current_x is not None:
                    response += "\n"
                response += f"📋 {row[column_x]}:\n"
                current_x = row[column_x]
            response += f"   • {row[column_y]}: {row['count']:,.0f}\n".replace(',', ' ')
        
        return response

    @staticmethod
    def format_time_series(result: pd.DataFrame, metric: str, group_by: Optional[str]) -> str:
        if len(result) == 0:
            return "❌ Нет данных для временного анализа"
        
        metric_name = METRIC_NAMES.get(metric, metric)
        response = f"📈 Динамика {metric_name}:\n\n"
        
        if group_by and group_by in result.columns:
            groups = pd.unique(result[group_by].dropna())
            for group in groups:
                group_data = result[result[group_by] == group]
                response += f"🏷️ {group}:\n"
                for _, row in group_data.iterrows():
                    date_str = str(row['report_date'])[:10]
                    value = row['value']
                    if metric == 'attrition_rate':
                        response += f"   • {date_str}: {value:.1f}%\n"
                    else:
                        response += f"   • {date_str}: {value:,.0f}\n".replace(',', ' ')
                response += "\n"
        else:
            for _, row in result.iterrows():
                date_str = str(row['report_date'])[:10]
                value = row['value']
                if metric == 'attrition_rate':
                    response += f"• {date_str}: {value:.1f}%\n"
                else:
                    response += f"• {date_str}: {value:,.0f}\n".replace(',', ' ')
        
        return response

    @staticmethod
    def format_top_values(result: pd.DataFrame, column_name: str, n: int) -> str:
        if len(result) == 0:
            return f"❌ Нет данных для анализа"
        
        column_names = {
            'location_name': 'локаций',
            'cluster': 'кластеров',
            'service': 'сервисов',
            'department_3': 'отделов',
            'age_category': 'возрастных категорий',
            'experience_category': 'категорий опыта',
            'fte': 'ставок'  # НОВОЕ: для топа ставок
        }
        
        display_name = column_names.get(column_name, column_name)
        
        header = f"🏆 Топ-{n} {display_name}"
        
        if len(result) > 0 and 'hirecount' in str(result.columns):
            header += " по найму:"
        else:
            header += " по численности:"
        
        response = f"{header}\n\n"
        
        for i, (_, row) in enumerate(result.iterrows(), 1):
            value = row[column_name]
            
            if column_name == 'fte':
                fte_names = {
                    0.0: "0.0 (нулевая)",
                    0.2: "0.2", 
                    0.37: "0.37",
                    0.4: "0.4",
                    0.5: "0.5 (половинная)",
                    0.75: "0.75",
                    1.0: "1.0 (полная)"
                }
                value = fte_names.get(value, f"{value}")
            
            response += f"{i}. {value}: {row['count']:,.0f}\n".replace(',', ' ')
        
        return response

    @staticmethod
    def format_unique_values(result: pd.DataFrame, column_name: str) -> str:
        if len(result) == 0:
            return f"❌ Нет данных в колонке '{column_name}'"
        
        values = result[column_name].tolist()
        
        column_names = {
            'location_name': 'локации',
            'cluster': 'кластеры', 
            'service': 'сервисы',
            'department_3': 'отделы 3 уровня',
            'sex': 'пол',
            'age_category': 'возрастные категории',
            'experience_category': 'категории опыта',
            'fte': 'ставки'  # НОВОЕ: для уникальных ставок
        }
        
        display_name = column_names.get(column_name, column_name)
        
        if len(values) > 10:
            response = f"📋 {display_name.capitalize()} (первые 10 из {len(values)}):\n\n"
            values = values[:10]
        else:
            response = f"📋 {display_name.capitalize()}:\n\n"
        
        for i, value in enumerate(values, 1):
            # НОВАЯ ЛОГИКА: Специальное форматирование для ставок
            if column_name == 'fte':
                fte_names = {
                    0.0: "0.0 (нулевая)",
                    0.2: "0.2", 
                    0.37: "0.37",
                    0.4: "0.4",
                    0.5: "0.5 (половинная)",
                    0.75: "0.75",
                    1.0: "1.0 (полная)"
                }
                value = fte_names.get(value, f"{value}")
            
            clean_value = str(value).replace('(Не исп)', '').replace('(не исп)', '').replace('(Не исп.)', '').strip()
            if clean_value:
                response += f"{i}. {clean_value}\n"
        
        if len(result) > 10:
            response += f"\n... и еще {len(result) - 10} других"
        
        return response