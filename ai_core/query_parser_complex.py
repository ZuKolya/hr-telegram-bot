# query_parser_complex.py
import re
from typing import Dict, Any

class ComplexQueryParser:
    def parse_complex_query(self, text: str) -> Dict[str, Any]:
        text_lower = text.lower()
        
        hiring_result = self._parse_hiring_needs(text_lower)
        if hiring_result:
            return hiring_result
        
        attrition_result = self._parse_attrition_analysis(text_lower)
        if attrition_result:
            return attrition_result
        
        segmentation_result = self._parse_deep_segmentation(text_lower)
        if segmentation_result:
            return segmentation_result
        
        risk_result = self._parse_risk_analysis(text_lower)
        if risk_result:
            return risk_result
        
        if any(word in text_lower for word in ['сравни', 'где выше', 'самый высок', 'где ниже', 'самый низк', 'меньше всего']):
            return self._parse_comparison_query(text_lower)
        
        if any(word in text_lower for word in ['тренд', 'динамик', 'изменил', 'изменен', 'рост', 'паден']):
            return self._parse_trend_query(text_lower)
        
        experience_age_result = self._parse_experience_age_query(text_lower)
        if experience_age_result:
            return experience_age_result
        
        return {"action": "unknown", "parameters": {}}

    def _parse_hiring_needs(self, text_lower: str) -> Dict[str, Any]:
        if any(word in text_lower for word in ['потребность в найме', 'нужно нанимать', 'необходимо нанимать', 'покрыть отток', 'закрыть отток', 'найм для покрытия', 'прогноз потребности', 'анализ эффективности найма']):
            
            if any(word in text_lower for word in ['в такси', 'в маркет', 'в крауд', 'в лавка', 'в финтех', 'в еда', 'в доставка', 'в облако', 'в коммерческий', 'в общие']):
                service = self._extract_service(text_lower)
                if service:
                    return {
                        "action": "calculate_hiring_needs", 
                        "parameters": {
                            "service": service,
                            "period": "month"
                        }
                    }
            
            return {
                "action": "calculate_hiring_needs", 
                "parameters": {
                    "period": "month"
                }
            }
        
        return None

    def _parse_attrition_analysis(self, text_lower: str) -> Dict[str, Any]:
        if any(word in text_lower for word in ['отток', 'текучест', 'увольнен', 'attrition']) and any(word in text_lower for word in ['демограф', 'возраст', 'опыт', 'пол', 'категори']):
            
            service = self._extract_service(text_lower)
            if not service:
                return {
                    "action": "unknown", 
                    "parameters": {"reason": "Укажите сервис для анализа оттока (например, 'анализ оттока в Такси по возрасту')"}
                }
            
            dimension = 'age_category'
            if 'возраст' in text_lower:
                dimension = 'age_category'
            elif 'опыт' in text_lower:
                dimension = 'experience_category'
            elif 'пол' in text_lower:
                dimension = 'sex'
            elif 'кластер' in text_lower:
                dimension = 'cluster'
            
            return {
                "action": "analyze_attrition_by_demography", 
                "parameters": {
                    "service": service,
                    "dimension": dimension
                }
            }
        
        return None

    def _parse_deep_segmentation(self, text_lower: str) -> Dict[str, Any]:
        if any(word in text_lower for word in ['сегмент', 'групп', 'категори', 'сегментирован']) and any(word in text_lower for word in ['глубок', 'многомерн', 'нескольк', 'комбинац']):
            
            segment_by = []
            
            if 'возраст' in text_lower and 'опыт' in text_lower:
                segment_by = ['age_category', 'experience_category']
            elif 'возраст' in text_lower and 'пол' in text_lower:
                segment_by = ['age_category', 'sex']
            elif 'опыт' in text_lower and 'пол' in text_lower:
                segment_by = ['experience_category', 'sex']
            elif 'сервис' in text_lower and 'возраст' in text_lower:
                segment_by = ['service', 'age_category']
            elif 'сервис' in text_lower and 'опыт' in text_lower:
                segment_by = ['service', 'experience_category']
            elif 'локац' in text_lower and 'возраст' in text_lower:
                segment_by = ['location_name', 'age_category']
            elif 'ставк' in text_lower and 'возраст' in text_lower:
                segment_by = ['fte', 'age_category']
            elif 'ставк' in text_lower and 'опыт' in text_lower:
                segment_by = ['fte', 'experience_category']
            else:
                segment_by = ['age_category', 'experience_category']
            
            metrics = ['headcount', 'attrition_rate']
            if 'ставк' in text_lower:
                metrics.append('avg_fte')
            if 'опыт' in text_lower and 'средн' in text_lower:
                metrics.append('avg_experience')
            if 'возраст' in text_lower and 'средн' in text_lower:
                metrics.append('avg_age')
            
            return {
                "action": "deep_segmentation_analysis", 
                "parameters": {
                    "segment_by": segment_by,
                    "metrics": metrics
                }
            }
        
        return None

    def _parse_risk_analysis(self, text_lower: str) -> Dict[str, Any]:
        if any(word in text_lower for word in ['риск', 'проблем', 'опасн', 'группа риск', 'высокий отток']):
            
            service = self._extract_service(text_lower)
            parameters = {}
            
            if service:
                parameters['service'] = service
            
            risk_factors = []
            if 'возраст' in text_lower:
                risk_factors.append('age_category')
            if 'опыт' in text_lower:
                risk_factors.append('experience_category')
            if 'пол' in text_lower:
                risk_factors.append('sex')
            if 'ставк' in text_lower or 'fte' in text_lower:
                risk_factors.append('fte')
            
            if risk_factors:
                parameters['risk_factors'] = risk_factors
            
            return {
                "action": "attrition_risk_analysis", 
                "parameters": parameters
            }
        
        return None

    def _parse_comparison_query(self, text_lower: str) -> Dict[str, Any]:
        dimension = 'service'
        
        if 'локац' in text_lower:
            dimension = 'location_name'
        elif 'кластер' in text_lower:
            dimension = 'cluster'
        elif 'возраст' in text_lower:
            dimension = 'age_category'
        elif 'опыт' in text_lower:
            dimension = 'experience_category'
        elif 'пол' in text_lower:
            dimension = 'sex'
        
        metric = 'headcount'
        
        if 'текучест' in text_lower:
            metric = 'turnover_rate'
        elif 'возраст' in text_lower and 'средн' in text_lower:
            metric = 'average_age'
        elif 'опыт' in text_lower and 'средн' in text_lower:
            metric = 'average_experience'
        elif 'ставк' in text_lower and 'средн' in text_lower:
            metric = 'average_fte'
        elif 'найм' in text_lower:
            metric = 'total_hired'
        elif 'увольн' in text_lower:
            metric = 'total_fired'
        
        if any(word in text_lower for word in ['низк', 'меньш', 'ниже', 'минимальн']):
            return {
                "action": "compare_metrics_min", 
                "parameters": {
                    "metric": metric, 
                    "dimension": dimension
                }
            }
        else:
            return {
                "action": "compare", 
                "parameters": {
                    "metric": metric, 
                    "dimension": dimension
                }
            }

    def _parse_trend_query(self, text_lower: str) -> Dict[str, Any]:
        metric = 'headcount'
        
        if 'найм' in text_lower:
            metric = 'total_hired'
        elif 'увольн' in text_lower:
            metric = 'total_fired'
        elif 'текучест' in text_lower:
            metric = 'turnover_rate'
        elif 'численност' in text_lower:
            metric = 'headcount'
        
        period = '3month'
        if 'все' in text_lower or 'весь' in text_lower:
            period = 'all'
        elif '1 месяц' in text_lower:
            period = '1month'
        
        return {
            "action": "trend_analysis", 
            "parameters": {
                "metric": metric, 
                "period": period
            }
        }

    def _parse_experience_age_query(self, text_lower: str) -> Dict[str, Any]:
        if any(word in text_lower for word in ['опыт', 'месяц', 'лет', 'год']):
            if 'меньше' in text_lower or '<' in text_lower:
                num_match = re.search(r'([0-9]+)\s*месяц', text_lower)
                if num_match:
                    months = int(num_match.group(1))
                    return {
                        "action": "calculate", 
                        "parameters": {
                            "metric": "headcount", 
                            "filters": {"experience": f"<{months}"}
                        }
                    }
                num_match = re.search(r'([0-9]+)\s*лет', text_lower)
                if num_match:
                    years = int(num_match.group(1))
                    return {
                        "action": "calculate", 
                        "parameters": {
                            "metric": "headcount", 
                            "filters": {"experience": f"<{years * 12}"}
                        }
                    }
                return {
                    "action": "calculate", 
                    "parameters": {
                        "metric": "headcount", 
                        "filters": {"experience": "<12"}
                    }
                }
            elif 'больше' in text_lower or '>' in text_lower or 'более' in text_lower:
                num_match = re.search(r'([0-9]+)\s*месяц', text_lower)
                if num_match:
                    months = int(num_match.group(1))
                    return {
                        "action": "calculate", 
                        "parameters": {
                            "metric": "headcount", 
                            "filters": {"experience": f">{months}"}
                        }
                    }
                num_match = re.search(r'([0-9]+)\s*лет', text_lower)
                if num_match:
                    years = int(num_match.group(1))
                    return {
                        "action": "calculate", 
                        "parameters": {
                            "metric": "headcount", 
                            "filters": {"experience": f">{years * 12}"}
                        }
                    }
                return {
                    "action": "calculate", 
                    "parameters": {
                        "metric": "headcount", 
                        "filters": {"experience": ">60"}
                    }
                }
            elif '3 месяц' in text_lower or 'три месяц' in text_lower:
                return {
                    "action": "calculate", 
                    "parameters": {
                        "metric": "headcount", 
                        "filters": {"experience": "<3"}
                    }
                }
            elif '5 лет' in text_lower or 'пять лет' in text_lower:
                return {
                    "action": "calculate_complex", 
                    "parameters": {
                        "metric": "experienced_workers"
                    }
                }
            elif '2 год' in text_lower or 'два год' in text_lower:
                return {
                    "action": "calculate", 
                    "parameters": {
                        "metric": "headcount", 
                        "filters": {"experience": ">24"}
                    }
                }
            elif '1 год' in text_lower or 'один год' in text_lower:
                return {
                    "action": "calculate", 
                    "parameters": {
                        "metric": "headcount", 
                        "filters": {"experience": ">12"}
                    }
                }
        
        if any(word in text_lower for word in ['возраст', 'лет', 'младш', 'старш', 'молод', 'пожил']):
            if 'меньше' in text_lower or '<' in text_lower or 'младш' in text_lower or 'молод' in text_lower:
                num_match = re.search(r'([0-9]+)\s*лет', text_lower)
                if num_match:
                    age = int(num_match.group(1))
                    return {
                        "action": "calculate", 
                        "parameters": {
                            "metric": "headcount", 
                            "filters": {"fullyears": f"<{age}"}
                        }
                    }
                if '25' in text_lower or 'двадцати пяти' in text_lower:
                    return {
                        "action": "calculate_complex", 
                        "parameters": {
                            "metric": "young_workers"
                        }
                    }
                if '30' in text_lower or 'тридцат' in text_lower:
                    return {
                        "action": "calculate", 
                        "parameters": {
                            "metric": "headcount", 
                            "filters": {"fullyears": "<30"}
                        }
                    }
                return {
                    "action": "calculate_complex", 
                    "parameters": {
                        "metric": "young_workers"
                    }
                }
            elif 'больше' in text_lower or '>' in text_lower or 'старш' in text_lower:
                num_match = re.search(r'([0-9]+)\s*лет', text_lower)
                if num_match:
                    age = int(num_match.group(1))
                    return {
                        "action": "calculate", 
                        "parameters": {
                            "metric": "headcount", 
                            "filters": {"fullyears": f">{age}"}
                        }
                    }
                if '40' in text_lower or 'сорок' in text_lower:
                    return {
                        "action": "calculate", 
                        "parameters": {
                            "metric": "headcount", 
                            "filters": {"fullyears": ">40"}
                        }
                    }
                if '50' in text_lower or 'пятьдесят' in text_lower:
                    return {
                        "action": "calculate", 
                        "parameters": {
                            "metric": "headcount", 
                            "filters": {"fullyears": ">50"}
                        }
                    }
                return {
                    "action": "calculate", 
                    "parameters": {
                        "metric": "headcount", 
                        "filters": {"fullyears": ">40"}
                    }
                }
        
        return None

    def _extract_service(self, text_lower: str) -> str:
        service_mapping = {
            'такси': 'Такси',
            'маркет': 'Маркет', 
            'крауд': 'Крауд',
            'лавка': 'Лавка',
            'финтех': 'Финтех',
            'еда': 'Еда',
            'доставка': 'Доставка',
            'облако': 'Облако',
            'коммерческий': 'Коммерческий департамент',
            'общие': 'Общие подразделения'
        }
        
        for key, value in service_mapping.items():
            if key in text_lower:
                return value
        
        return None

    def _build_filters_from_text(self, text_lower: str) -> Dict[str, Any]:
        filters = {}
        
        for service in ['такси', 'маркет', 'крауд', 'лавка', 'финтех', 'еда', 'доставка', 'облако']:
            if service in text_lower:
                filters['service'] = service.capitalize()
                break
        
        if 'москв' in text_lower:
            filters['location_name'] = '%Москва%'
        elif 'питер' in text_lower or 'петербург' in text_lower or 'спб' in text_lower:
            filters['location_name'] = '%Санкт-Петербург%'
        elif 'новосибирск' in text_lower:
            filters['location_name'] = '%Новосибирск%'
        elif 'екатеринбург' in text_lower:
            filters['location_name'] = '%Екатеринбург%'
        
        if 'женщин' in text_lower or 'женск' in text_lower:
            filters['sex'] = 'F'
        elif 'мужчин' in text_lower or 'мужск' in text_lower:
            filters['sex'] = 'M'
        
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
        
        return filters