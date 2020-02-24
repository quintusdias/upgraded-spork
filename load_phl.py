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
        self.df = pd.read_csv('phl_exoplanet_catalog.csv')

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
            id               serial primary key, 
            name             text,
            mass             double precision,
            mass_error_min   double precision,
            mass_error_max   double precision,
            radius           double precision,
            radius_error_min double precision,
            radius_error_max double precision,
            year             integer,
            star_id          integer,
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

        sql = """
        comment on column planet_lut.name is 'planet name'
        """
        self.cursor.execute(sql)

        sql = """
        comment on column planet_lut.mass is 'earth masses'
        """
        self.cursor.execute(sql)

        sql = """
        comment on column planet_lut.mass_error_min is 'earth masses'
        """
        self.cursor.execute(sql)

        sql = """
        comment on column planet_lut.mass_error_max is 'earth masses'
        """
        self.cursor.execute(sql)

        sql = """
        comment on column planet_lut.radius is 'earth radii'
        """
        self.cursor.execute(sql)

        sql = """
        comment on column planet_lut.radius_error_min is 'earth radii'
        """
        self.cursor.execute(sql)

        sql = """
        comment on column planet_lut.radius_error_max is 'earth radii'
        """
        self.cursor.execute(sql)

        sql = """
        comment on column planet_lut.year is 'planet discovered year'
        """
        self.cursor.execute(sql)

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
            year
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
            %(year_discovered)s
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
