from src.core.util.factory import Factory
from src.filemanager.fmanager import FileReader, FileWriter, TemplateFileWriter


def initialize():
    factory = Factory()
    factory.register("filemanager.filereader", FileReader)
    factory.register("filemanager.filewriter", FileWriter)
    factory.register("filemanager.templatewriter", TemplateFileWriter)
