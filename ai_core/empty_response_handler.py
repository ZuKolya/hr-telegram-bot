# empty_response_handler.py
from typing import Dict

class EmptyResponseHandler:
    def categorize_empty_response(self, tool_result: str, command: Dict) -> str:
        tool_result_lower = tool_result.lower()
        
        if "нет данных для сервиса" in tool_result_lower:
            return "unknown_service"
        elif "недостаточно данных для анализа рисков" in tool_result_lower:
            return "insufficient_risk_data"
        elif "сегментационный анализ временно недоступен" in tool_result_lower:
            return "feature_unavailable"
        elif "корреляционный анализ временно недоступен" in tool_result_lower:
            return "feature_unavailable"
        elif "анализ рисков временно недоступен" in tool_result_lower:
            return "feature_unavailable"
        elif "укажите сервис для расчета потребности в найме" in tool_result_lower:
            return "missing_service_hiring"
        elif "нет данных" in tool_result_lower:
            return "no_data_general"
        elif "не указана метрика" in tool_result_lower:
            return "missing_metric"
        elif "не указана колонка" in tool_result_lower:
            return "missing_column"
        elif "колонка" in tool_result_lower and "не найдена" in tool_result_lower:
            return "unknown_column"
        elif "метод" in tool_result_lower and "не поддерживается" in tool_result_lower:
            return "unsupported_method"
        elif "метрика" in tool_result_lower and "не поддерживается" in tool_result_lower:
            return "unsupported_metric"
        else:
            return "other_error"