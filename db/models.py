from sqlalchemy import Column, Integer, String, Numeric, TIMESTAMP, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Country(Base):
    __tablename__ = 'country'
    country_id = Column(Integer, primary_key=True)
    country_name = Column(String(150), nullable=False)
    iso_code = Column(String(10))

class EnergyStats(Base):
    __tablename__ = 'energy_stats'
    stats_id = Column(Integer, primary_key=True)
    country_id = Column(Integer, ForeignKey('country.country_id'), nullable=False)
    year = Column(Integer, nullable=False)
    population = Column(Numeric)
    gdp = Column(Numeric)
    primary_energy_consumption = Column(Numeric)
    oil_production = Column(Numeric)
    oil_consumption = Column(Numeric)
    gas_consumption = Column(Numeric)
    coal_consumption = Column(Numeric)
    nuclear_consumption = Column(Numeric)
    renewables_consumption = Column(Numeric)
    fossil_fuel_consumption = Column(Numeric)
    carbon_intensity_elec = Column(Numeric)
    renewables_share_energy = Column(Numeric)
    oil_share_energy = Column(Numeric)
    fossil_share_energy = Column(Numeric)
    created_at = Column(TIMESTAMP)
