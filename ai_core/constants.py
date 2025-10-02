#constants.py:
SUPPORTED_METRICS = [
    "headcount", "turnover_rate", "average_experience", "average_age", 
    "average_fte", "total_fired", "total_hired",
    "fte_distribution", "remote_workers", "full_time_ratio", "part_time_ratio",
    "young_workers", "experienced_workers", "remote_ratio"
]

TIME_SERIES_METRICS = [
    "headcount", "turnover", "hires", "attrition_rate", 
    "total_hired", "total_fired", "retention_rate"
]

COMPLEX_ACTIONS = ["compare", "trend_analysis", "correlation", "segmentation", "benchmark"]

METRIC_NAMES = {
    'headcount': 'Численность',
    'turnover': 'Увольнения', 
    'hires': 'Наймы',
    'attrition_rate': 'Уровень текучести (%)',
    'total_hired': 'Наймы',
    'total_fired': 'Увольнения',
    'turnover_rate': 'Уровень текучести',
    'average_experience': 'Средний опыт',
    'average_age': 'Средний возраст',
    'average_fte': 'Средняя ставка',
    'retention_rate': 'Уровень удержания',
    'diversity_index': 'Индекс разнообразия',
    'full_time_ratio': 'Доля полных ставок',
    'part_time_ratio': 'Доля частичной ставки',
    'fte_distribution': 'Распределение ставок',
    'remote_workers': 'Удаленные сотрудники',
    'young_workers': 'Молодые сотрудники',
    'experienced_workers': 'Опытные сотрудники',
    'remote_ratio': 'Доля удаленных сотрудников'
}

DIMENSIONS = ['service', 'location_name', 'cluster', 'age_category', 'experience_category', 'sex']

NUMERIC_FILTERS = ['experience', 'fullyears', 'fte']
CATEGORICAL_FILTERS = ['service', 'location_name', 'cluster', 'sex', 'age_category', 'experience_category']

FTE_VALUES = [0.0, 0.2, 0.37, 0.4, 0.5, 0.75, 1.0]

EXPERIENCE_THRESHOLDS = {
    'junior': 12,      # до 1 года
    'middle': 60,      # 1-5 лет  
    'senior': 120      # более 5 лет
}

AGE_THRESHOLDS = {
    'young': 25,       # до 25 лет
    'middle': 40,      # 25-40 лет
    'senior': 60       # старше 40 лет
}

AGE_CATEGORIES = {
    '18-25 лет': '18-25 лет',
    '25-40 лет': '25-40 лет', 
    '40-60 лет': '40-60 лет',
    '60+ лет': '60+ лет'
}

EXPERIENCE_CATEGORIES = {
    'менее 1 года': 'менее 1 года',
    '1-3 года': '1-3 года',
    '3-5 лет': '3-5 лет', 
    'более 5 лет': 'более 5 лет'
}