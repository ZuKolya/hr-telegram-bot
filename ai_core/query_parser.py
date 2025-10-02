# query_parser.py
import json
import re
from typing import Dict, Any, Optional

class QueryParser:
    def _classify_query_complexity(self, user_query: str) -> str:
        query_lower = user_query.lower()
        
        simple_patterns = [
            'сколько', 'численност', 'работает', 'сотрудник',
            'распределен', 'статистик', 'stats', 
            'уникальн', 'какие', 'топ', 'top',
            'средн', 'avg', 'средний', 'средняя'
        ]
        
        complex_patterns = [
            'сравни', 'тренд', 'динамик', 'изменил', 
            'где выше', 'самый высок', 'где ниже', 'самый низк',
            'корреляц', 'сегментац', 'риск', 'проблем'
        ]
        
        if any(pattern in query_lower for pattern in complex_patterns):
            return "complex"
        
        return "simple"

    def _is_complex_command(self, command: Dict, user_query: str) -> bool:
        action = command.get('action', '')
        
        complex_actions = ['compare', 'trend_analysis', 'correlation', 'segmentation', 'risk_assessment']
        
        if action in complex_actions:
            return True
        
        return False

    def _fix_json_errors(self, json_str: str) -> str:
        json_str = re.sub(r'\s+', ' ', json_str)
        
        try:
            json.loads(json_str)
            return json_str
        except json.JSONDecodeError:
            pass
        
        json_str = re.sub(r'"service":\s*"null"', '"service": null', json_str)
        json_str = re.sub(r'"filters":\s*"null"', '"filters": {}', json_str)
        
        open_braces = json_str.count('{')
        close_braces = json_str.count('}')
        if open_braces > close_braces:
            json_str += '}' * (open_braces - close_braces)
        
        json_str = re.sub(r',\s*}', '}', json_str)
        json_str = re.sub(r',\s*]', ']', json_str)
        
        return json_str

    def _parse_llm_response(self, llm_response: str) -> Dict[str, Any]:
        try:
            cleaned = llm_response.strip()
            
            if '```json' in cleaned:
                cleaned = cleaned.replace('```json', '').replace('```', '')
            elif '```' in cleaned:
                cleaned = cleaned.replace('```', '')
            
            try:
                parsed = json.loads(cleaned)
                if isinstance(parsed, dict) and 'action' in parsed:
                    return parsed
                elif isinstance(parsed, dict):
                    if 'column' in parsed and 'n' in parsed:
                        return {"action": "top_values", "parameters": parsed}
                    elif 'action' in parsed:
                        return parsed
                    else:
                        return {"action": "calculate", "parameters": parsed}
            except json.JSONDecodeError:
                pass
            
            json_match = re.search(r'\{[^{}]*\{[^{}]*\}[^{}]*\}|\{[^{}]*\}', cleaned, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                
                for attempt in range(3):
                    try:
                        parsed = json.loads(json_str)
                        if isinstance(parsed, dict) and 'action' in parsed:
                            return parsed
                        elif isinstance(parsed, dict):
                            if 'column' in parsed and 'n' in parsed:
                                return {"action": "top_values", "parameters": parsed}
                            elif 'action' in parsed:
                                return parsed
                            else:
                                return {"action": "calculate", "parameters": parsed}
                    except json.JSONDecodeError as e:
                        if attempt == 0:
                            json_str = self._fix_json_errors(json_str)
                        elif attempt == 1:
                            json_str = re.sub(r',\s*"filters":\s*\{[^}]*\}', '', json_str)
                            json_str = re.sub(r',\s*"sort":[^,]+', '', json_str)
                            json_str = re.sub(r',\s*"limit":[^,]+', '', json_str)
                        else:
                            action_match = re.search(r'"action":\s*"([^"]+)"', json_str)
                            if action_match:
                                return {"action": action_match.group(1), "parameters": {}}
                            break
            
            return self._manual_parse_response(cleaned)
            
        except Exception as e:
            print(f"❌ Other parsing error: {e}")
            return self._manual_parse_response(llm_response)

    def _manual_parse_response(self, text: str) -> Dict[str, Any]:
        from .query_parser_complex import ComplexQueryParser
        complex_parser = ComplexQueryParser()
        complex_result = complex_parser.parse_complex_query(text)
        
        if complex_result and complex_result.get('action') != 'unknown':
            return complex_result
        
        return self._parse_simple_query(text)

    def _parse_simple_query(self, text: str) -> Dict[str, Any]:
        text_lower = text.lower()
        
        if any(phrase in text_lower for phrase in ['какие задать вопросы', 'что можно спросить', 'что ты понимаешь', 'примеры запросов', 'какие бывают вопросы']):
            return {
                "action": "help_examples",
                "parameters": {}
            }
        
        if any(phrase in text_lower for phrase in ['какие вопросы', 'что спросить', 'примеры вопросов']):
            return {
                "action": "help_examples",
                "parameters": {}
            }
        
        if any(word in text_lower for word in ['возрастн', 'age_categor']):
            if 'распределен' in text_lower or 'категори' in text_lower:
                return {
                    "action": "get_stats", 
                    "parameters": {"column": "age_category"}
                }
            age_category_mapping = {
                '25-40': '25-40 лет',
                '18-25': '18-25 лет', 
                '40-60': '40-60 лет',
                '60+': '60+ лет'
            }
            for key, value in age_category_mapping.items():
                if key in text_lower:
                    return {
                        "action": "calculate", 
                        "parameters": {
                            "metric": "headcount", 
                            "filters": {"age_category": value}
                        }
                    }
        
        if any(word in text_lower for word in ['опыт работ', 'experience_categor']):
            if 'распределен' in text_lower or 'категори' in text_lower:
                return {
                    "action": "get_stats", 
                    "parameters": {"column": "experience_category"}
                }
        
        if any(word in text_lower for word in ['ставк', 'fte', '0.5', '0.2', '1.0', '0.0']):
            if 'распределен' in text_lower or 'сколько на каждой' in text_lower or 'доля' in text_lower:
                return {
                    "action": "get_stats",
                    "parameters": {"column": "fte"}
                }
            elif '0.5' in text_lower:
                return {
                    "action": "calculate", 
                    "parameters": {
                        "metric": "headcount", 
                        "filters": {"fte": 0.5}
                    }
                }
            elif '0.2' in text_lower:
                return {
                    "action": "calculate", 
                    "parameters": {
                        "metric": "headcount", 
                        "filters": {"fte": 0.2}
                    }
                }
            elif '1.0' in text_lower or 'полн' in text_lower:
                return {
                    "action": "calculate", 
                    "parameters": {
                        "metric": "headcount", 
                        "filters": {"fte": 1.0}
                    }
                }
            elif '0.0' in text_lower or 'нулев' in text_lower:
                return {
                    "action": "calculate", 
                    "parameters": {
                        "metric": "headcount", 
                        "filters": {"fte": 0.0}
                    }
                }
            elif any(str(i) in text_lower for i in [2, 3, 4, 5, 6, 7, 8, 9]):
                return {
                    "action": "unknown", 
                    "parameters": {
                        "reason": "Ставки могут быть только от 0.0 до 1.0. Используйте значения: 0.0, 0.2, 0.5, 1.0"
                    }
                }
        
        if any(word in text_lower for word in ['удален', 'дистанц', 'remote', 'дистанционщик']):
            if 'сколько' in text_lower or 'численност' in text_lower:
                return {
                    "action": "calculate", 
                    "parameters": {
                        "metric": "remote_workers"
                    }
                }
            else:
                return {
                    "action": "calculate", 
                    "parameters": {
                        "metric": "headcount", 
                        "filters": {"location_name": "%Дистанционщик%"}
                    }
                }
        
        action = None
        if any(word in text_lower for word in ['топ', 'top', 'самые', 'крупн']):
            action = "top_values"
        elif any(word in text_lower for word in ['найм', 'наня', 'hiring']):
            action = "calculate"
        elif any(word in text_lower for word in ['увольн', 'текучест', 'turnover', 'уволен']):
            action = "calculate"
        elif any(word in text_lower for word in ['сколько', 'численност', 'сотрудник', 'работает']):
            action = "calculate"
        elif any(word in text_lower for word in ['распределен', 'статистик', 'stats']):
            action = "get_stats"
        elif any(word in text_lower for word in ['уникальн', 'какие']):
            action = "unique_values"
        else:
            action = "unknown"
        
        parameters = {}
        
        if 'текучест' in text_lower:
            parameters['metric'] = 'turnover_rate'
        elif 'найм' in text_lower or 'наня' in text_lower:
            parameters['metric'] = 'total_hired'
        elif 'увольн' in text_lower:
            parameters['metric'] = 'total_fired'
        elif 'численност' in text_lower or 'сколько' in text_lower or 'сотрудник' in text_lower or 'работает' in text_lower:
            parameters['metric'] = 'headcount'
        elif 'возраст' in text_lower and ('средн' in text_lower or 'avg' in text_lower):
            parameters['metric'] = 'average_age'
        elif 'опыт' in text_lower and ('средн' in text_lower or 'avg' in text_lower):
            parameters['metric'] = 'average_experience'
        elif 'ставк' in text_lower and ('средн' in text_lower or 'avg' in text_lower):
            parameters['metric'] = 'average_fte'
        
        if 'сервис' in text_lower:
            parameters['column'] = 'service'
        elif 'локац' in text_lower or 'город' in text_lower:
            parameters['column'] = 'location_name'
        elif 'кластер' in text_lower:
            parameters['column'] = 'cluster'
        elif 'пол' in text_lower or 'женщин' in text_lower or 'мужчин' in text_lower:
            parameters['column'] = 'sex'
        elif 'возраст' in text_lower:
            parameters['column'] = 'age_category'
        elif 'опыт' in text_lower:
            parameters['column'] = 'experience_category'
        
        filters = {}
        
        for service in ['такси', 'маркет', 'крауд', 'лавка', 'финтех', 'еда', 'доставка', 'облако']:
            if service in text_lower:
                filters['service'] = service.capitalize()
                break
        
        if 'москв' in text_lower:
            filters['location_name'] = '%Москва%'
        elif 'питер' in text_lower or 'петербург' in text_lower or 'спб' in text_lower:
            filters['location_name'] = '%Санкт-Петербург%'
        
        if 'ставк' in text_lower or 'fte' in text_lower:
            if '0.5' in text_lower:
                filters['fte'] = 0.5
            elif 'полн' in text_lower or '1.0' in text_lower or '1,0' in text_lower:
                filters['fte'] = 1.0
            elif '0.2' in text_lower or '0,2' in text_lower:
                filters['fte'] = 0.2
            elif '0.0' in text_lower or '0,0' in text_lower or 'нулев' in text_lower or ' 0 ' in text_lower:
                filters['fte'] = 0.0
        
        if any(word in text_lower for word in ['удален', 'дистанц', 'remote']):
            filters['location_name'] = "%Дистанционщик%"
        
        if action == "top_values":
            n_match = re.search(r'топ[-\s]*(\d+)', text_lower)
            parameters['n'] = int(n_match.group(1)) if n_match else 20
            
            if 'column' not in parameters:
                parameters['column'] = 'service'
            
            if 'женщин' in text_lower or 'женск' in text_lower:
                if 'filters' not in parameters:
                    parameters['filters'] = {}
                parameters['filters']['sex'] = 'F'
            elif 'мужчин' in text_lower or 'мужск' in text_lower:
                if 'filters' not in parameters:
                    parameters['filters'] = {}
                parameters['filters']['sex'] = 'M'
        
        if filters:
            if 'filters' in parameters:
                parameters['filters'].update(filters)
            else:
                parameters['filters'] = filters
        
        return {
            "action": action,
            "parameters": parameters
        }

    def _infer_metric_from_query(self, user_query: str) -> str:
        query_lower = user_query.lower()
        
        if 'ставк' in query_lower and not ('средн' in query_lower or 'avg' in query_lower):
            return 'headcount'
        
        if 'опыт' in query_lower and not ('средн' in query_lower or 'avg' in query_lower):
            return 'headcount'
        
        if any(word in query_lower for word in ['текучест', 'turnover']):
            return 'turnover_rate'
        elif any(word in query_lower for word in ['найм', 'наня', 'hiring']):
            return 'total_hired'
        elif any(word in query_lower for word in ['увольн', 'fired']):
            return 'total_fired'
        elif any(word in query_lower for word in ['сколько', 'численност', 'сотрудник', 'работает']):
            return 'headcount'
        elif any(word in query_lower for word in ['возраст', 'age']) and ('средн' in query_lower or 'avg' in query_lower):
            return 'average_age'
        elif any(word in query_lower for word in ['опыт', 'experience']) and ('средн' in query_lower or 'avg' in query_lower):
            return 'average_experience'
        elif any(word in query_lower for word in ['ставк', 'fte']) and ('средн' in query_lower or 'avg' in query_lower):
            return 'average_fte'
        
        return 'headcount'