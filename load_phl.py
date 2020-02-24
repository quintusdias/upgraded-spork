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

        self.create_stars()
        self.load_stars()

        self.update()

        self.create_planets()
        self.load_planets()

    def update(self):
        df_stars = pd.read_sql('select * from star_lut', self.conn)

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

    def create_planets(self):
        sql = """
        drop table if exists planet_lut
        """
        self.cursor.execute(sql)

        sql = """
        create table planet_lut (
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
            star_id                   integer,
            unique(name)
        )
        """
        self.cursor.execute(sql)

        sql = """
        alter  table planet_lut
        add constraint parent_star
            foreign key (star_id)
            references star_lut(id)
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
            'semi_major_axis_error_min': 'planet semi_major_axis min (AU)',
            'semi_major_axis_error_max': 'planet semi_major_axis max (AU)',
        }
        for key, value in column_comments.items():
            sql = f"""
            comment on column planet_lut.{key} is '{value}'
            """
            self.cursor.execute(sql, {'value': value})

    def load_planets(self):

        sql = """
        insert into planet_lut
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
            semi_major_axis_error_max
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
            %(p_semi_major_axis_error_max)s
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
            }
            self.cursor.execute(sql, params)

    def create_stars(self):
        sql = """
        drop table if exists star_lut cascade
        """
        self.cursor.execute(sql)

        sql = """
        create table star_lut (
            id   serial primary key, 
            name text,
            unique(name)
        )
        """
        self.cursor.execute(sql)

        sql = """
        comment on column star_lut.name is 'star name'
        """
        self.cursor.execute(sql)

    def load_stars(self):
        stars = self.df['s_name'].unique()

        sql = """
        insert into star_lut (name)
        values (%(name)s)
        """

        for star in stars:
            self.cursor.execute(sql, {'name': star})

if __name__ == '__main__':
    o = Thang()
    o.run()
