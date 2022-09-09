from dotenv import dotenv_values

config = dotenv_values(".env")


class Config:

    @staticmethod
    def get_val(name):
        return config[name]
