from utils import testing


if __name__ == '__main__':
    testing.list_attrs()
    print(testing.use_value())
    testing.MyVariable = 'Hello world'
    print(testing.use_value())
    testing.set_value('Hello')
    print(testing.use_value())




