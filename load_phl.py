import logging
import sys

import pandas as pd
import psycopg2
import sqlalchemy


class Thang(object):

    def __init__(self):
        self.engine = sqlalchemy.create_engine('postgresql:///phl')
        self.conn = psycopg2.connect(dbname='phl')
        self.cursor = self.conn.cursor()
        self.setup_logging()

    def setup_logging(self):
        """
        Parameters
        ----------
        verbosity : str
            Logging level
        """
        level = logging.INFO

        logger = logging.getLogger('loadphl')
        logger.setLevel(level)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(format)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        self.logger = logger

    def __del__(self):
        self.conn.commit()

    def check_historically_empty_columns(self):
        vars = [
            's_disc',
            's_magnetic_field',
        ]
        for varname in vars:
            s = self.df[varname].isnull().sum()
            if s < self.df.shape[0]:
                self.logger.warning(f"{varname} now has some data")

    def preprocess(self):
        self.logger.info('Pre-processing...')
        self.check_historically_empty_columns()

    def postprocess(self):
        self.check_for_bad_star_ages()

    def check_for_bad_star_ages(self):
        sql = """
        select id
        from stars
        where age < 0
        """
        df = pd.read_sql(sql, self.conn)

        if df.shape[0] > 0:
            n = len(df)
            ids = df.id.values
            msg = f"found {n} rows where star age is negative, IDs = ({ids})"
            self.logger.warn(msg)

            sql = """
                  update stars
                  set age = 'NaN'
                  where age < 0
                  """
            self.cursor.execute(sql)

    def run(self):
        self.load_data()
        self.preprocess()
        self.create_constellations()
        self.create_stars()
        self.create_planets()
        self.postprocess()

    def create_planets(self):
        self.logger.info('Creating planets ...')
        self.define_planets()
        self.load_planets()
        self.logger.info('Done with planets ...')

    def create_stars(self):
        self.logger.info('Creating stars ...')
        self.define_stars()
        self.load_stars()
        self.retrieve_star_id()
        self.logger.info('Done with stars ...')

    def create_constellations(self):
        self.logger.info('Creating constellations ...')
        self.define_constellations()
        self.load_constellations()
        self.logger.info('Done with constellations ...')

    def retrieve_star_id(self):
        """
        get rid of the star name now that we have the ID.
        """
        df_stars = pd.read_sql('select * from stars', self.conn)

        df = pd.merge(self.df, df_stars, how='inner', left_on='s_name', right_on='name')
        df = df.drop('s_name', axis='columns')
        df = df.rename(mapper={'id': 'star_id'}, axis='columns')

        self.df = df

    def load_data(self):
        self.logger.info('starting to read data from CSV file')
        self.df = pd.read_csv('phl_exoplanet_catalog.csv',
                              parse_dates=['P_UPDATED'])
        self.logger.info('finished reading data from CSV file')

        # Make the columns all lower case.  Upper case causes issues.
        cols = [x.lower() for x in self.df.columns]
        self.df.columns = cols

    def define_constellations(self):
        sql = """
        drop table if exists constellations cascade
        """
        self.cursor.execute(sql)

        sql = """
        create table constellations (
            id                        serial primary key, 
            name                      text,
            abr                       text,
            meaning                   text
        )
        """
        self.cursor.execute(sql)

        column_comments = {
            'abr':              'constellation abreviated',
        }
        for key, value in column_comments.items():
            sql = f"""
            comment on column constellations.{key} is '{value}'
            """
            self.cursor.execute(sql, {'value': value})

    def define_planets(self):
        sql = """
        drop table if exists planets
        """
        self.cursor.execute(sql)

        sql = """
        create table planets (
            id                        serial primary key, 
            name                      text,
            mass                      real,
            mass_error_min            real,
            mass_error_max            real,
            radius                    real,
            radius_error_min          real,
            radius_error_max          real,
            year_discovered           integer,
            last_updated              timestamp,
            period                    real,
            period_error_min          real,
            period_error_max          real,
            semi_major_axis           real,
            semi_major_axis_error_min real,
            semi_major_axis_error_max real,
            eccentricity              real,
            eccentricity_error_min    real,
            eccentricity_error_max    real,
            inclination               real,
            inclination_error_min     real,
            inclination_error_max     real,
            omega                     real,
            omega_error_min           real,
            omega_error_max           real,
            tperi                     real,
            tperi_error_min           real,
            tperi_error_max           real,
            angular_distance          real,
            impact_parameter           real,
            impact_parameter_error_min real,
            impact_parameter_error_max real,
            temp_measured             real,
            geo_albedo                real,
            geo_albedo_error_min      real,
            geo_albedo_error_max      real,
            detection                 text,
            detection_mass            text,
            detection_radius          real,
            alt_names                 text,
            atmosphere                text,
            type                      text,
            escape                    real,
            potential                 real,
            gravity                   real,
            density                   real,
            hill_sphere               real,
            distance                  real,
            periastron                real,
            apastron                  real,
            distance_eff              real,
            flux                      real,
            flux_min                  real,
            flux_max                  real,
            temp_equil                real,
            temp_equil_min            real,
            temp_equil_max            real,
            star_id                   integer,
            unique(name)
        )
        """
        self.cursor.execute(sql)

        sql = """
        alter  table planets
        add constraint parent_star
            foreign key (star_id)
            references stars(id)
        """
        self.cursor.execute(sql)

        column_comments = {
            'name':             'planet name',
            'mass':             'earth masses',
            'mass_error_min':   'earth masses',
            'mass_error_max':   'earth masses',
            'radius':           'earth radii',
            'radius_error_min': 'earth radii',
            'radius_error_max': 'earth radii',
            'year_discovered':  'discovered year',
            'period':           'period (days)',
            'period_error_min': 'period min (days)',
            'period_error_max': 'period max (days)',
            'semi_major_axis':           'semi_major_axis (AU)',
            'semi_major_axis_error_min': 'semi_major_axis error min (AU)',
            'semi_major_axis_error_max': 'semi_major_axis error max (AU)',
            'eccentricity':              'eccentricity',
            'eccentricity_error_min':    'eccentricity error min',
            'eccentricity_error_max':    'eccentricity error max',
            'inclination':               'inclination (deg)',
            'inclination_error_min':     'inclination error min (deg)',
            'inclination_error_max':     'inclination error max (deg)',
            'omega':                     'argument of periastron (deg)',
            'omega_error_min':           'argument of periastron error min (deg)',
            'omega_error_max':           'argument of periastron error max (deg)',
            'tperi':                     'of periastron (seconds)',
            'tperi_error_min':           'time of periastron error min (seconds)',
            'tperi_error_max':           'time of periastron error max (seconds)',
            'impact_parameter':            'impact parameter',
            'impact_parameter_error_min':  'impact parameter error min',
            'impact_parameter_error_max':  'impact parameter error max',
            'angular_distance':          'planet-star angular separation (arcsec)',
            'temp_measured':             'measured equilibrium temperature (K)',
            'geo_albedo':                'measured geometric albedo',
            'geo_albedo_error_min':      'measured geometric albedo error min',
            'geo_albedo_error_max':      'measured geometric albedo error max',
            'detection':                 'detection method',
            'detection_mass':            'detection method for mass',
            'detection_radius':          'detection method for radius',
            'alt_names':                 'alternate names',
            'atmosphere':                'atmosphere composition (no data yet)',
            'type':                      'planet type (PHL''s mass-radius classification)',
            'escape':                    'escape velocity (earth units)',
            'potential':                 'gravitational potential (earth units)',
            'gravity':                   'gravity (earth units)',
            'density':                   'density (earth units)',
            'hill_sphere':               'hill sphere (AU)',
            'distance':                  'planet mean distance from star (AU)',
            'periastron':                'periastron (AU)',
            'apastron':                  'apastron (AU)',
            'distance_eff':              'effective thermal distance from star (AU)',
            'flux':                      'planet mean stellar flux (earth units)',
            'flux_min':                  'planet minimum orbital stellar flux (earth units)',
            'flux_max':                  'planet maximum orbital stellar flux (earth units)',
            'temp_equil':                'equilibrium temperature assuming bond albedo 0.3 (K)',
            'temp_equil_min':            'equilibrium minimum temperature assuming bond albedo 0.3 (K)',
            'temp_equil_max':            'equilibrium maximum temperature assuming bond albedo 0.3 (K)',
        }
        for key, value in column_comments.items():
            sql = f"""
            comment on column planets.{key} is '{value}'
            """
            self.cursor.execute(sql, {'value': value})

    def load_planets(self):

        sql = """
        insert into planets
        (
            name,
            star_id,
            mass,
            mass_error_min,
            mass_error_max,
            radius,
            radius_error_min,
            radius_error_max,
            year_discovered,
            period,
            period_error_min,
            period_error_max,
            semi_major_axis,
            semi_major_axis_error_min,
            semi_major_axis_error_max,
            eccentricity,
            eccentricity_error_min,
            eccentricity_error_max,
            inclination,
            inclination_error_min,
            inclination_error_max,
            omega,
            omega_error_min,
            omega_error_max,
            tperi,
            tperi_error_min,
            tperi_error_max,
            impact_parameter,
            impact_parameter_error_min,
            impact_parameter_error_max,
            angular_distance,
            temp_measured,
            geo_albedo,
            geo_albedo_error_min,
            geo_albedo_error_max,
            detection,
            detection_mass,
            detection_radius,
            alt_names,
            atmosphere,
            type,
            escape,
            potential,
            gravity,
            density,
            hill_sphere,
            distance,
            periastron,
            apastron,
            distance_eff,
            flux,
            flux_min,
            flux_max,
            temp_equil,
            temp_equil_min,
            temp_equil_max
        )
        values
        (
            %(name)s,
            %(star_id)s,
            %(mass)s,
            %(mass_error_min)s,
            %(mass_error_max)s,
            %(radius)s,
            %(radius_error_min)s,
            %(radius_error_max)s,
            %(year_discovered)s,
            %(p_period)s,
            %(p_period_error_min)s,
            %(p_period_error_max)s,
            %(p_semi_major_axis)s,
            %(p_semi_major_axis_error_min)s,
            %(p_semi_major_axis_error_max)s,
            %(p_eccentricity)s,
            %(p_eccentricity_error_min)s,
            %(p_eccentricity_error_max)s,
            %(p_inclination)s,
            %(p_inclination_error_min)s,
            %(p_inclination_error_max)s,
            %(p_omega)s,
            %(p_omega_error_min)s,
            %(p_omega_error_max)s,
            %(p_tperi)s,
            %(p_tperi_error_min)s,
            %(p_tperi_error_max)s,
            %(p_impact_parameter)s,
            %(p_impact_parameter_error_min)s,
            %(p_impact_parameter_error_max)s,
            %(p_angular_distance)s,
            %(p_temp_measured)s,
            %(p_geo_albedo)s,
            %(p_geo_albedo_error_min)s,
            %(p_geo_albedo_error_max)s,
            %(p_detection)s,
            %(p_detection_mass)s,
            %(p_detection_radius)s,
            %(p_alt_names)s,
            %(p_atmosphere)s,
            %(p_type)s,
            %(p_escape)s,
            %(p_potential)s,
            %(p_gravity)s,
            %(p_density)s,
            %(p_hill_sphere)s,
            %(p_distance)s,
            %(p_periastron)s,
            %(p_apastron)s,
            %(p_distance_eff)s,
            %(p_flux)s,
            %(p_flux_min)s,
            %(p_flux_max)s,
            %(p_temp_equil)s,
            %(p_temp_equil_min)s,
            %(p_temp_equil_max)s
        )
        """

        for idx, row in self.df.iterrows():
            params = {
                'name': row['p_name'],
                'star_id': row['star_id'],
                'mass': row['p_mass'],
                'mass_error_min': row['p_mass_error_min'],
                'mass_error_max': row['p_mass_error_max'],
                'radius': row['p_radius'],
                'radius_error_min': row['p_radius_error_min'],
                'radius_error_max': row['p_radius_error_max'],
                'year_discovered': row['p_year'],
                'p_period': row['p_period'],
                'p_period_error_min': row['p_period_error_min'],
                'p_period_error_max': row['p_period_error_max'],
                'p_semi_major_axis': row['p_semi_major_axis'],
                'p_semi_major_axis_error_min': row['p_semi_major_axis_error_min'],
                'p_semi_major_axis_error_max': row['p_semi_major_axis_error_max'],
                'p_eccentricity': row['p_eccentricity'],
                'p_eccentricity_error_min': row['p_eccentricity_error_min'],
                'p_eccentricity_error_max': row['p_eccentricity_error_max'],
                'p_inclination': row['p_inclination'],
                'p_inclination_error_min': row['p_inclination_error_min'],
                'p_inclination_error_max': row['p_inclination_error_max'],
                'p_omega': row['p_omega'],
                'p_omega_error_min': row['p_omega_error_min'],
                'p_omega_error_max': row['p_omega_error_max'],
                'p_tperi': row['p_tperi'],
                'p_tperi_error_min': row['p_tperi_error_min'],
                'p_tperi_error_max': row['p_tperi_error_max'],
                'p_impact_parameter': row['p_impact_parameter'],
                'p_impact_parameter_error_min': row['p_impact_parameter_error_min'],
                'p_impact_parameter_error_max': row['p_impact_parameter_error_max'],
                'p_angular_distance': row['p_angular_distance'],
                'p_temp_measured': row['p_temp_measured'],
                'p_geo_albedo': row['p_geo_albedo'],
                'p_geo_albedo_error_min': row['p_geo_albedo_error_min'],
                'p_geo_albedo_error_max': row['p_geo_albedo_error_max'],
                'p_detection': row['p_detection'],
                'p_detection_mass': row['p_detection_mass'],
                'p_detection_radius': row['p_detection_radius'],
                'p_alt_names': row['p_alt_names'],
                'p_atmosphere': row['p_atmosphere'],
                'p_type': row['p_type'],
                'p_escape': row['p_escape'],
                'p_potential': row['p_potential'],
                'p_gravity': row['p_gravity'],
                'p_density': row['p_density'],
                'p_hill_sphere': row['p_hill_sphere'],
                'p_distance': row['p_distance'],
                'p_periastron': row['p_periastron'],
                'p_apastron': row['p_apastron'],
                'p_distance_eff': row['p_distance_eff'],
                'p_flux': row['p_flux'],
                'p_flux_min': row['p_flux_min'],
                'p_flux_max': row['p_flux_max'],
                'p_temp_equil': row['p_temp_equil'],
                'p_temp_equil_min': row['p_temp_equil_min'],
                'p_temp_equil_max': row['p_temp_equil_max'],
            }
            self.cursor.execute(sql, params)

    def load_constellations(self):
        cols = ['s_constellation', 's_constellation_abr', 's_constellation_eng']
        df = self.df[cols].drop_duplicates()

        sql = """
        insert into constellations (name, abr, meaning)
        values %s
        """

        arglist = [row.to_dict() for _, row in df.iterrows()]
        template = (
            '(%(s_constellation)s, '
            '%(s_constellation_abr)s, '
            '%(s_constellation_eng)s)'
        )
        psycopg2.extras.execute_values(self.cursor, sql, arglist, template)

    def define_stars(self):
        sql = """
        drop table if exists stars cascade
        """
        self.cursor.execute(sql)

        sql = """
        create table stars (
            id                    serial primary key, 
            name                  text,
            ra                    real,
            dec                   real,
            mag                   real,
            distance              real,
            distance_error_min    real,
            distance_error_max    real,
            metallicity           real,
            metallicity_error_min real,
            metallicity_error_max real,
            mass                  real,
            mass_error_min        real,
            mass_error_max        real,
            radius                real,
            radius_error_min      real,
            radius_error_max      real,
            type                  text,
            age                   real,
            age_error_min         real,
            age_error_max         real,
            temperature           real,
            temperature_error_min real,
            temperature_error_max real,
            log_g                 real,
            alt_names             text,
            constellation_id      integer,
            unique(name)
        )
        """
        self.cursor.execute(sql)

        sql = """
        alter  table stars
        add constraint parent_constellation
            foreign key (constellation_id)
            references constellations(id)
        """
        self.cursor.execute(sql)

        column_comments = {
            'name':                  'star name',
            'constellation_id':      'link back to constellation table',
            'ra':                    'right ascension (decimal deg)',
            'dec':                   'declination (decimal deg)',
            'mag':                   'magnitude',
            'distance':              'distance (parsecs)',
            'distance_error_min':    'distance error min (parsecs)',
            'distance_error_max':    'distance error max (parsecs)',
            'metallicity':           'metallicity (parsecs)',
            'metallicity_error_min': 'metallicity error min (parsecs)',
            'metallicity_error_max': 'metallicity error max (parsecs)',
            'mass':                  'mass (solar units)',
            'mass_error_min':        'mass error min (solar units)',
            'mass_error_max':        'mass error max (solar units)',
            'radius':                'radius (solar units)',
            'radius_error_min':      'radius error min (solar units)',
            'radius_error_max':      'radius error max (solar units)',
            'age':                'age (Gy)',
            'age_error_min':      'age error min (Gy)',
            'age_error_max':      'age error max (Gy)',
            'temperature':                'effective temperature (K)',
            'temperature_error_min':      'effective temperature error min (K)',
            'temperature_error_max':      'effective temperature error max (K)',
            'log_g':               'log(g)',
            'alt_names':           'alternative names',
            'type':                  'star spectral type',
        }

        for key, value in column_comments.items():
            sql = f"""
            comment on column stars.{key} is '{value}'
            """
            self.cursor.execute(sql, {'value': value})

    def load_stars(self):

        columns = [
            's_name',
            's_constellation',
            's_ra',
            's_dec',
            's_mag',
            's_distance',
            's_distance_error_min',
            's_distance_error_max',
            's_metallicity',
            's_metallicity_error_min',
            's_metallicity_error_max',
            's_mass',
            's_mass_error_min',
            's_mass_error_max',
            's_radius',
            's_radius_error_min',
            's_radius_error_max',
            's_age',
            's_age_error_min',
            's_age_error_max',
            's_temperature',
            's_temperature_error_min',
            's_temperature_error_max',
            's_log_g',
            's_alt_names',
            's_type',
        ]
        stars = self.df[columns].drop_duplicates()

        constellations = pd.read_sql('select * from constellations', self.conn)
        df = pd.merge(stars, constellations, how='inner', left_on='s_constellation', right_on='name') 

        sql = """
        insert into stars
        (
            name,
            constellation_id,
            ra,
            dec,
            mag,
            distance,
            distance_error_min,
            distance_error_max,
            metallicity,
            metallicity_error_min,
            metallicity_error_max,
            mass,
            mass_error_min,
            mass_error_max,
            radius,
            radius_error_min,
            radius_error_max,
            age,
            age_error_min,
            age_error_max,
            temperature,
            temperature_error_min,
            temperature_error_max,
            log_g,
            alt_names,
            type
        )
        values %s
        """

        arglist = [row.to_dict() for _, row in df.iterrows()]
        template = (
            '(%(s_name)s, '
            '%(id)s, '
            '%(s_ra)s, '
            '%(s_dec)s, '
            '%(s_mag)s, '
            '%(s_distance)s, '
            '%(s_distance_error_min)s, '
            '%(s_distance_error_max)s, '
            '%(s_metallicity)s, '
            '%(s_metallicity_error_min)s, '
            '%(s_metallicity_error_max)s, '
            '%(s_mass)s, '
            '%(s_mass_error_min)s, '
            '%(s_mass_error_max)s, '
            '%(s_radius)s, '
            '%(s_radius_error_min)s, '
            '%(s_radius_error_max)s, '
            '%(s_age)s, '
            '%(s_age_error_min)s, '
            '%(s_age_error_max)s, '
            '%(s_temperature)s, '
            '%(s_temperature_error_min)s, '
            '%(s_temperature_error_max)s, '
            '%(s_log_g)s, '
            '%(s_alt_names)s, '
            '%(s_type)s) '
        )

        psycopg2.extras.execute_values(self.cursor, sql, arglist, template)

if __name__ == '__main__':
    o = Thang()
    o.run()
