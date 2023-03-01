import random


class MaskData:
    JP_CHOICE = "これはマスクされたデータです森田亮太かまごめホヤゆみゆままと "
    EN_CHOICE = "Abcdefghijklmnopqr stuvwxyZ"
    NUMBER_CHOICE = "0123456789"

    @staticmethod
    def get_masked_data(field_value: str):
        length = len(field_value)
        choice = MaskData.JP_CHOICE
        if field_value.isnumeric():
            choice = MaskData.NUMBER_CHOICE
        else:
            if field_value.isascii():
                choice = MaskData.EN_CHOICE
        masked_data = ''.join(random.choice(choice) for i in range(length))
        return masked_data
