"""
Manufacturer name standardization mapping
Used across all NHTSA data cleaning scripts
"""

VALID_MANUFACTURERS = [
    "Tesla", "Ford", "Toyota", "GMC",
    "Honda", "Chevrolet", "Nissan", "BMW", 
    "Mercedes-Benz", "Volkswagen", "Hyundai", 
    "Kia", "Subaru", "Mazda", "Jeep"
]

 
MANUFACTURER_MAPPING = {
    # Tesla variations
    'TESLA': 'Tesla',
    'TESLA MOTORS': 'Tesla',
    'TESLA, INC.': 'Tesla',
    # Ford variations
    'FORD': 'Ford',
    'FORD MOTOR COMPANY': 'Ford',
    'FORD MOTOR CO.': 'Ford',
    # Toyota variations
    'TOYOTA': 'Toyota',
    'TOYOTA MOTOR CORPORATION': 'Toyota',
    'TOYOTA MOTOR CORP': 'Toyota',
    # General Motors variations
    'GENERAL MOTORS': 'General Motors',
    'GM': 'General Motors',
    'GMC': 'General Motors',
    'GENERAL MOTORS LLC': 'General Motors',
    'GENERAL MOTORS CORPORATION': 'General Motors',
    # Honda variations
    'HONDA': 'Honda',
    'HONDA MOTOR CO.': 'Honda',
    'HONDA MOTOR COMPANY': 'Honda',
    # Chevrolet variations
    'CHEVROLET': 'Chevrolet',
    'CHEVY': 'Chevrolet',
    'CHEVROLET MOTOR DIVISION': 'Chevrolet',
    # Nissan variations
    'NISSAN': 'Nissan',
    'NISSAN MOTOR COMPANY': 'Nissan',
    'NISSAN NORTH AMERICA': 'Nissan',
    # BMW variations
    'BMW': 'BMW',
    'BMW OF NORTH AMERICA': 'BMW',
    'BAYERISCHE MOTOREN WERKE': 'BMW',
    # Mercedes-Benz variations
    'MERCEDES-BENZ': 'Mercedes-Benz',
    'MERCEDES BENZ': 'Mercedes-Benz',
    'DAIMLER': 'Mercedes-Benz',
    'DAIMLER AG': 'Mercedes-Benz',
    'MERCEDES-BENZ USA': 'Mercedes-Benz',
    # Volkswagen variations
    'VOLKSWAGEN': 'Volkswagen',
    'VW': 'Volkswagen',
    'VOLKSWAGEN GROUP': 'Volkswagen',
    'VOLKSWAGEN OF AMERICA': 'Volkswagen',
    # Hyundai variations
    'HYUNDAI': 'Hyundai',
    'HYUNDAI MOTOR COMPANY': 'Hyundai',
    'HYUNDAI MOTOR AMERICA': 'Hyundai',
    # Kia variations
    'KIA': 'Kia',
    'KIA MOTORS': 'Kia',
    'KIA MOTORS AMERICA': 'Kia',
    # Subaru variations
    'SUBARU': 'Subaru',
    'SUBARU OF AMERICA': 'Subaru',
    'FUJI HEAVY INDUSTRIES': 'Subaru',
    # Mazda variations
    'MAZDA': 'Mazda',
    'MAZDA MOTOR CORPORATION': 'Mazda',
    'MAZDA NORTH AMERICA': 'Mazda',
    # Jeep variations
    'JEEP': 'Jeep',
    'FCA US LLC': 'Jeep',
    'CHRYSLER': 'Jeep',
    'STELLANTIS': 'Jeep',
}
 
def get_standardized_manufacturer(make_name):
    """
    Get standardized manufacturer name
    Args:
        make_name: Original manufacturer name
    Returns:
        Standardized manufacturer name or original if not found
    """
    if not make_name:
        return None
    make_upper = str(make_name).upper().strip()
    return MANUFACTURER_MAPPING.get(make_upper, make_name)