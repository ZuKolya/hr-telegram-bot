# ai_assistant.py
import aiohttp
import json
import re
import time
import asyncio
from typing import Dict, Any, Optional
from config import YANDEX_API_KEY, YANDEX_FOLDER_ID, AI_TIMEOUT
from ai_core.agent_tools import AgentTools
from ai_core.agent_tools_complex import ComplexAgentTools
from ai_core.query_parser import QueryParser
from ai_core.query_parser_complex import ComplexQueryParser
from ai_core.response_handler import ResponseHandler
from ai_core.empty_response_handler import EmptyResponseHandler
from ai_core.prompts import SYSTEM_PROMPT

class AIAssistant:
    def __init__(self, db_path: str):
        self.api_key = YANDEX_API_KEY
        self.folder_id = YANDEX_FOLDER_ID
        self.tools = AgentTools(db_path)
        self.complex_tools = ComplexAgentTools(db_path)
        self.parser = QueryParser()
        self.complex_parser = ComplexQueryParser()
        self.handler = ResponseHandler()
        self.empty_handler = EmptyResponseHandler()
        self.model_name = "yandexgpt-lite"

    async def _call_yandex_gpt(self, user_prompt: str) -> str:
        url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Api-Key {self.api_key}",
            "x-folder-id": self.folder_id
        }
        
        payload = {
            "modelUri": f"gpt://{self.folder_id}/{self.model_name}",
            "completionOptions": {
                "stream": False,
                "temperature": 0.1,
                "maxTokens": 1000
            },
            "messages": [
                {
                    "role": "system",
                    "text": SYSTEM_PROMPT
                },
                {
                    "role": "user", 
                    "text": user_prompt
                }
            ]
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload, timeout=AI_TIMEOUT) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result['result']['alternatives'][0]['message']['text']
                    else:
                        error_text = await response.text()
                        raise Exception(f"API Error: {response.status} - {error_text}")
        except asyncio.TimeoutError:
            raise Exception("Таймаут запроса к Yandex GPT API")
        except Exception as e:
            raise Exception(f"Ошибка подключения к Yandex GPT: {str(e)}")

    async def process_query(self, user_query: str) -> str:
        try:
            general_response = self.handler._handle_general_questions(user_query)
            if general_response:
                return general_response
            
            complexity = self.parser._classify_query_complexity(user_query)
            
            print(f"🔍 QUERY COMPLEXITY: '{complexity}' for: '{user_query}'")
            
            llm_response = await self._call_yandex_gpt(user_query)
            print(f"🤖 RAW LLM RESPONSE: '{llm_response}'")
            
            llm_command = self.parser._parse_llm_response(llm_response)
            print(f"📋 PARSED COMMAND: {llm_command}")
            
            if not llm_command or llm_command.get('action') == 'unknown':
                return self.handler._handle_unknown_query(user_query)
            
            if self.parser._is_complex_command(llm_command, user_query):
                print("🔄 Complex command detected, using complex tools")
            
            tool_result = await self._execute_command(llm_command, user_query)
            print(f"🛠️ TOOL RESULT: {tool_result}")
            
            if tool_result.startswith("❌"):
                category = self.empty_handler.categorize_empty_response(tool_result, llm_command)
                return self.handler._handle_empty_response(category, user_query, llm_command)
            
            final_answer = self.handler._format_final_answer(user_query, tool_result, llm_command)
            
            return final_answer
            
        except Exception as e:
            print(f"❌ Error in process_query: {e}")
            return f"❌ Эйчарик запутался: {str(e)}\n\nПопробуй переформулировать вопрос или спроси что-то попроще."

    async def _execute_command(self, command: Dict[str, Any], user_query: str = "") -> str:
        action = command.get('action')
        parameters = command.get('parameters', {})
        
        print(f"🔧 Executing: {action} with params: {parameters}")
        
        try:
            if action == "help_examples":
                return self.handler._get_help_examples()
            
            complex_actions = [
                'compare', 'compare_metrics_min', 'trend_analysis', 
                'correlation', 'segmentation', 'risk_assessment',
                'calculate_complex',
                'analyze_attrition_by_demography', 'deep_segmentation_analysis',
                'attrition_risk_analysis', 'calculate_hiring_needs'
            ]
            
            if action in complex_actions:
                tools = self.complex_tools
                print("🔄 Using COMPLEX tools")
            else:
                tools = self.tools
                print("🔄 Using BASIC tools")
            
            if action == "calculate" and 'metric' not in parameters:
                metric = self.parser._infer_metric_from_query(user_query)
                if metric:
                    parameters['metric'] = metric
                else:
                    return "❌ Не указана метрика для расчета"
            
            user_query_lower = user_query.lower()
            use_min_comparison = any(word in user_query_lower for word in ['меньше', 'низк', 'младш', 'молод', 'мал', 'нижн', 'меньше всего', 'самые молодые', 'самая низкая'])
            
            if action == "get_stats":
                column = parameters.get('column')
                if not column:
                    return "❌ Не указана колонка для анализа"
                filters = parameters.get('filters', {})
                return await tools.get_column_statistics(column, filters, user_query)
            
            elif action == "calculate":
                metric = parameters.get('metric')
                if not metric:
                    return "❌ Не указана метрика для расчета"
                filters = parameters.get('filters', {})
                return await tools.calculate_metric(metric, filters)
            
            elif action == "time_series":
                metric = parameters.get('metric')
                if not metric:
                    return "❌ Не указана метрика для временного анализа"
                group_by = parameters.get('group_by')
                if group_by:
                    return "❌ Группировка в временных рядах временно недоступна"
                filters = parameters.get('filters', {})
                return await tools.time_series_analysis(metric, None, filters)
            
            elif action == "unique_values":
                column = parameters.get('column')
                if not column:
                    return "❌ Не указана колонка для получения уникальных значений"
                return await tools.get_unique_values(column)
            
            elif action == "top_values":
                column = parameters.get('column')
                n = parameters.get('n', 20)
                if not column:
                    return "❌ Не указана колонка для анализа"
                filters = parameters.get('filters', {})
                return await tools.get_top_values(column, n, filters)
            
            elif action == "compare":
                metric = parameters.get('metric')
                dimension = parameters.get('dimension')
                if not metric or not dimension:
                    return "❌ Для сравнения нужны metric и dimension"
                filters = parameters.get('filters', {})
                
                if use_min_comparison:
                    result = await self.complex_tools.compare_metrics_min(metric, dimension, filters)
                else:
                    result = await self.complex_tools.compare_metrics(metric, dimension, filters)
                
                if metric == 'average_experience':
                    result = result.replace('  ', ' ')
                    result = re.sub(r'(\d+\.\s+[^:]+:\s*)(\d+\.?\d*)', r'\1\2 месяцев', result)
                
                return result
            
            elif action == "compare_metrics_min":
                metric = parameters.get('metric')
                dimension = parameters.get('dimension')
                if not metric or not dimension:
                    return "❌ Для сравнения нужны metric и dimension"
                filters = parameters.get('filters', {})
                result = await self.complex_tools.compare_metrics_min(metric, dimension, filters)
                
                if metric == 'average_experience':
                    result = result.replace('  ', ' ')
                    result = re.sub(r'(\d+\.\s+[^:]+:\s*)(\d+\.?\d*)', r'\1\2 месяцев', result)
                
                return result
            
            elif action == "trend_analysis":
                metric = parameters.get('metric')
                period = parameters.get('period', '3month')
                if not metric:
                    return "❌ Для анализа трендов нужна metric"
                filters = parameters.get('filters', {})
                return await self.complex_tools.trend_analysis(metric, period, filters)
            
            elif action == "calculate_complex":
                metric = parameters.get('metric')
                if not metric:
                    return "❌ Не указана метрика для расчета"
                filters = parameters.get('filters', {})
                return await self.complex_tools.calculate_complex_metric(metric, filters)
            
            elif action == "correlation":
                return await self.complex_tools.correlation_analysis(
                    parameters.get('metric1'), 
                    parameters.get('metric2'), 
                    parameters.get('filters', {})
                )
            
            elif action == "segmentation":
                return await self.complex_tools.segmentation_analysis(
                    parameters.get('segment_by'),
                    parameters.get('metrics', []),
                    parameters.get('filters', {})
                )
            
            elif action == "risk_assessment":
                return await self.complex_tools.risk_assessment(parameters.get('filters', {}))
            
            elif action == "analyze_attrition_by_demography":
                service = parameters.get('service')
                dimension = parameters.get('dimension')
                if not service or not dimension:
                    return "❌ Для анализа оттока нужны service и dimension"
                filters = parameters.get('filters', {})
                return await self.complex_tools.analyze_attrition_by_demography(service, dimension, filters)
            
            elif action == "deep_segmentation_analysis":
                segment_by = parameters.get('segment_by', [])
                metrics = parameters.get('metrics', [])
                if not segment_by:
                    return "❌ Для глубокой сегментации нужен segment_by"
                filters = parameters.get('filters', {})
                return await self.complex_tools.deep_segmentation_analysis(segment_by, metrics, filters)
            
            elif action == "attrition_risk_analysis":
                service = parameters.get('service')
                risk_factors = parameters.get('risk_factors')
                filters = parameters.get('filters', {})
                return await self.complex_tools.attrition_risk_analysis(service, risk_factors, filters)
            
            elif action == "calculate_hiring_needs":
                service = parameters.get('service')
                period = parameters.get('period', 'month')
                filters = parameters.get('filters', {})
                return await self.complex_tools.calculate_hiring_needs(service, period, filters)
            
            elif action == "unknown":
                reason = parameters.get('reason', 'Неизвестная причина')
                return f"❌ Не могу обработать запрос: {reason}"
            
            else:
                return f"❌ Неизвестная команда: {action}"
                
        except Exception as e:
            print(f"❌ Command execution error: {e}")
            return f"❌ Ошибка выполнения команды: {str(e)}"

    async def test_connection(self) -> bool:
        try:
            test_response = await self._call_yandex_gpt("Тестовый запрос")
            return bool(test_response and len(test_response) > 0)
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False

AIAssistantCore = AIAssistant