import os
from pathlib import Path

# Nous définissons ici la liste EXACTE des pays et leur région.
# C'est la "Vérité Terrain".
CSV_CONTENT = """country,region
Afghanistan,Asia
Albania,Europe
Algeria,Africa
Angola,Africa
Anguilla,South America
Antigua and Barbuda,South America
Argentina,South America
Armenia,Asia
Aruba,South America
Australia,Oceania
Austria,Europe
Azerbaijan,Asia
Bahamas,South America
Bahrain,Asia
Bangladesh,Asia
Barbados,South America
Belarus,Europe
Belgium,Europe
Belize,South America
Benin,Africa
Bermuda,South America
Bhutan,Asia
Bolivia,South America
Bosnia and Herzegovina,Europe
Botswana,Africa
Brazil,South America
Brunei Darussalam,Asia
Bulgaria,Europe
Burkina Faso,Africa
Burundi,Africa
Cabo Verde,Africa
Cambodia,Asia
Cameroon,Africa
Canada,North America
Cayman Islands,South America
Central African Republic,Africa
Chad,Africa
Chile,South America
China,Asia
Colombia,South America
Congo,Africa
"Congo, Democratic Republic of the",Africa
Costa Rica,South America
Côte d'Ivoire,Africa
Croatia,Europe
Cuba,South America
Cyprus,Asia
Czech Republic,Europe
Denmark,Europe
Djibouti,Africa
Dominican Republic,South America
Ecuador,South America
Egypt,Africa
El Salvador,South America
Equatorial Guinea,Africa
Eritrea,Africa
Estonia,Europe
Eswatini,Africa
Ethiopia,Africa
Faroe Islands,Europe
Fiji,Oceania
Finland,Europe
France,Europe
Gabon,Africa
Gambia,Africa
Georgia,Asia
Germany,Europe
Ghana,Africa
Greece,Europe
Greenland,North America
Guatemala,South America
Guernsey,Europe
Guinea,Africa
Guyana,South America
Haiti,South America
Honduras,South America
Hong Kong,Asia
Hungary,Europe
Iceland,Europe
India,Asia
Indonesia,Asia
Iran,Asia
Iraq,Asia
Ireland,Europe
Isle of Man,Europe
Israel,Asia
Italy,Europe
Jamaica,South America
Japan,Asia
Jordan,Asia
Kazakhstan,Asia
Kenya,Africa
Kiribati,Oceania
North Korea,Asia
South Korea,Asia
Kuwait,Asia
Kyrgyzstan,Asia
Lao People's Democratic Republic,Asia
Latvia,Europe
Lebanon,Asia
Lesotho,Africa
Liberia,Africa
Libya,Africa
Lithuania,Europe
Luxembourg,Europe
Macao,Asia
Madagascar,Africa
Malawi,Africa
Malaysia,Asia
Maldives,Asia
Mali,Africa
Malta,Europe
Mauritania,Africa
Mauritius,Africa
Mexico,North America
Moldova,Europe
Monaco,Europe
Mongolia,Asia
Montenegro,Europe
Morocco,Africa
Mozambique,Africa
Myanmar,Asia
Namibia,Africa
Nepal,Asia
Netherlands,Europe
New Zealand,Oceania
Nicaragua,South America
Niger,Africa
Nigeria,Africa
North Macedonia,Europe
Norway,Europe
Oman,Asia
Pakistan,Asia
Panama,South America
Papua New Guinea,Oceania
Paraguay,South America
Peru,South America
Philippines,Asia
Poland,Europe
Portugal,Europe
Puerto Rico,South America
Qatar,Asia
Romania,Europe
Russia,Europe
Rwanda,Africa
Saint Kitts and Nevis,South America
Saint Lucia,South America
Saint Vincent and the Grenadines,South America
Samoa,Oceania
San Marino,Europe
Saudi Arabia,Asia
Senegal,Africa
Serbia,Europe
Seychelles,Africa
Sierra Leone,Africa
Singapore,Asia
Slovakia,Europe
Slovenia,Europe
Solomon Islands,Oceania
Somalia,Africa
South Africa,Africa
South Sudan,Africa
Spain,Europe
Sri Lanka,Asia
Sudan,Africa
Suriname,South America
Sweden,Europe
Switzerland,Europe
Syrian Arab Republic,Asia
Taiwan,Asia
Tajikistan,Asia
Tanzania,Africa
Thailand,Asia
Timor-Leste,Asia
Togo,Africa
Tonga,Oceania
Trinidad and Tobago,South America
Tunisia,Africa
Turkey,Asia
Turkmenistan,Asia
Turks and Caicos Islands,South America
Uganda,Africa
Ukraine,Europe
United Arab Emirates,Asia
United Kingdom,Europe
United States,North America
Uruguay,South America
Uzbekistan,Asia
Vanuatu,Oceania
Venezuela,South America
Vietnam,Asia
Yemen,Asia
Zambia,Africa
Zimbabwe,Africa
Cocos (Keeling) Islands,Oceania
French Polynesia,Oceania
New Caledonia,Oceania
Svalbard and Jan Mayen,Europe
Reunion,Africa
Curacao,South America
"""

def generate_fixed_mapping():
    print(" Génération du fichier de mapping corrigé...")
    
    # Détection du chemin racine
    base_dir = Path(__file__).resolve().parent.parent if "scripts" in str(Path(__file__).parent) else Path(__file__).resolve().parent

    # Dossier cible : data/ref
    target_dir = base_dir / "data" / "ref"
    target_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = target_dir / "country_region_mapping.csv"

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(CSV_CONTENT.strip())
        print(f"✅ Fichier RECRÉÉ avec succès : {output_file}")
    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    generate_fixed_mapping()