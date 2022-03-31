from manipulating_data import ManipulatingData
from download_files import DownloadFiles


def main():
    # dwn_obj = DownloadFiles()
    # dwn_obj.download_files()
    obj = ManipulatingData()
    obj.download_files()
    obj.run_manipulations_spark()


main()
