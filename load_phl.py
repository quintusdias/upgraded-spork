import pandas as pd
import psycopg2
import sqlalchemy


class Thang(object):

    def __init__(self):
        self.engine = sqlalchemy.create_engine('postgresql:///phl')
        self.conn = psycopg2.connect(dbname='phl')
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.conn.commit()

    def run(self):
        self.load_data()

        self.create_constellations()
        self.load_constellations()

        self.define_stars()
        self.load_stars()

        self.update()

        self.create_planets()
        self.load_planets()

    def update(self):
        df_stars = pd.read_sql('select * from stars', self.conn)

        # get rid of the star name now that we have the ID.
        df = pd.merge(self.df, df_stars, how='inner', left_on='s_name', right_on='name')
        df = df.drop('s_name', axis='columns')
        df = df.rename(mapper={'id': 'star_id'}, axis='columns')

        self.df = df

    def load_data(self):
        self.df = pd.read_csv('phl_exoplanet_catalog.csv',
                              parse_dates=['P_UPDATED'])

        # Make the columns all lower case.  Upper case causes issues.
        cols = [x.lower() for x in self.df.columns]
        self.df.columns = cols

    def create_constellations(self):
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

    def create_planets(self):
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
            'year_discovered':  'planet discovered year',
            'period':           'planet period (days)',
            'period_error_min': 'planet period min (days)',
            'period_error_max': 'planet period max (days)',
            'semi_major_axis':           'planet semi_major_axis (AU)',
            'semi_major_axis_error_min': 'planet semi_major_axis error min (AU)',
            'semi_major_axis_error_max': 'planet semi_major_axis error max (AU)',
            'eccentricity':              'planet eccentricity',
            'eccentricity_error_min':    'planet eccentricity error min',
            'eccentricity_error_max':    'planet eccentricity error max',
            'inclination':               'planet inclination (deg)',
            'inclination_error_min':     'planet inclination error min (deg)',
            'inclination_error_max':     'planet inclination error max (deg)',
            'omega':                     'planet argument of periastron (deg)',
            'omega_error_min':           'planet argument of periastron error min (deg)',
            'omega_error_max':           'planet argument of periastron error max (deg)',
            'tperi':                     'planet time of periastron (seconds)',
            'tperi_error_min':           'planet time of periastron error min (seconds)',
            'tperi_error_max':           'planet time of periastron error max (seconds)',
            'impact_parameter':            'planet impact parameter',
            'impact_parameter_error_min':  'planet impact parameter error min',
            'impact_parameter_error_max':  'planet impact parameter error max',
            'angular_distance':          'planet-star angular separation (arcsec)',
            'temp_measured':             'planet measured equilibrium temperature (K)',
            'geo_albedo':                'planet measured geometric albedo',
            'geo_albedo_error_min':      'planet measured geometric albedo error min',
            'geo_albedo_error_max':      'planet measured geometric albedo error max',
            'detection':                 'planet detection method',
            'detection_mass':            'planet detection method for mass',
            'detection_radius':          'planet detection method for radius',
            'alt_names':                 'planet alternate names',
            'atmosphere':                'planet atmosphere composition (no data yet)',
            'type':                      'planet type (PHL''s mass-radius classification)',
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
            type
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
            %(p_type)s
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
            }
            self.cursor.execute(sql, params)

    def define_stars(self):
        sql = """
        drop table if exists stars cascade
        """
        self.cursor.execute(sql)

        sql = """
        create table stars (
            id               serial primary key, 
            name             text,
            constellation_id integer,
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

        sql = """
        comment on column stars.name is 'star name'
        """
        self.cursor.execute(sql)

        sql = (
            "comment on column stars.constellation_id is "
            "'link back to constellation table'"
        )
        self.cursor.execute(sql)

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

    def load_stars(self):

        columns = ['s_name', 's_constellation']
        stars = self.df[columns].drop_duplicates()

        constellations = pd.read_sql('select * from constellations', self.conn)
        df = pd.merge(stars, constellations, how='inner', left_on='s_constellation', right_on='name') 

        sql = """
        insert into stars (name, constellation_id)
        values %s
        """

        arglist = [row.to_dict() for _, row in df.iterrows()]
        template = '(%(s_name)s, %(id)s)'

        psycopg2.extras.execute_values(self.cursor, sql, arglist, template)

if __name__ == '__main__':
    o = Thang()
    o.run()
