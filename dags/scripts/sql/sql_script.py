class SQLQueries:
    """Class with all the sql query variables
    """

    """Temporary Table Queries"""

    create_temp_user = """CREATE TABLE IF NOT EXISTS public.temp_user(
                            user_id INT NOT NULL PRIMARY KEY,
                            name VARCHAR(50),
                            review_count INT,
                            yelping_since DATE,
                            average_stars REAL
                        )"""
                        
    create_temp_business = """CREATE TEMP TABLE IF NOT EXISTS temp_business(
                                business_id INT NOT NULL PRIMARY KEY,
                                name VARCHAR(50),
                                city VARCHAR(50),
                                state VARCHAR(50),
                                stars REAL,
                                review_count INT,
                                is_open INT
                            )"""

    create_temp_review = """CREATE TEMP TABLE IF NOT EXISTS temp_review(
                                business_id INT NOT NULL REFERENCES temp_business(business_id),
                                user_id INT NOT NULL REFERENCES temp_user(user_id),
                                date DATE,
                                review TEXT
                            )"""

    create_temp_tip = """CREATE TEMP TABLE IF NOT EXISTS temp_tip(
                            business_id INT NOT NULL REFERENCES temp_business(business_id),
                            user_id INT NOT NULL REFERENCES temp_user(user_id),
                            date DATE,
                            tip TEXT
                        )"""

    """Truncate temp tables"""

    trunc_temp_user = """TRUNCATE temp_user"""
    trunc_temp_business = """TRUNCATE temp_business"""
    trunc_temp_review = """TRUNCATE temp_review"""
    trunc_temp_tip = """TRUNCATE temp_tip"""

    """Create permanent tables"""

    create_table_user = """CREATE TABLE IF NOT EXISTS public.user(
                            user_id INT NOT NULL PRIMARY KEY,
                            name VARCHAR(50),
                            review_count INT,
                            yelping_since DATE,
                            average_stars REAL
                        )"""

    create_table_business = """CREATE TABLE IF NOT EXISTS public.business(
                                business_id INT NOT NULL PRIMARY KEY,
                                name VARCHAR(100),
                                city VARCHAR(50),
                                state VARCHAR(50),
                                stars REAL,
                                review_count INT,
                                is_open INT
                            )"""

    create_table_review =   """CREATE TABLE IF NOT EXISTS public.review(
                                business_id INT NOT NULL REFERENCES public.business(business_id),
                                user_id INT NOT NULL REFERENCES public.user(user_id),
                                date DATE,
                                review VARCHAR(MAX)
                            )"""

    create_table_tip =  """CREATE TABLE IF NOT EXISTS public.tip(
                            business_id INT NOT NULL REFERENCES public.business(business_id),
                            user_id INT NOT NULL REFERENCES public.user(user_id),
                            date DATE,
                            tip VARCHAR(MAX)
                        )"""
    