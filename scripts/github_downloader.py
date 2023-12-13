# this module takes care of fetching the github repository to our code base
from pathlib import Path
import subprocess
class GitDownloader:
    def __init__(self, repo_link):
        self.repo_link = repo_link
        self.local_dir = Path('tmp') / 'data' 

    def __get_files_glob(self):
        """
        Fetch the list of view_files and model_file(s)
        """
        self.view_file_glob = [*self._views.glob("*.lkml")]
        self.model_file_glob = [*self._models.glob('*.model.lkml')]
        
    def download_content(self):
        """
        this function takes care of a clone and a pull
        """
        subprocess.run(["git","clone",self.repo_link,self.local_dir])
        
        self._views = self.local_dir / "views"
        self._models = self.local_dir / "models"

        # for my use case it was kind of a compulsion to have a model file inside the models folder
        if not self._views.is_dir() and not self._models.is_dir():
            print("Download failed retry please make sure views are present in view folder and models are present in models folder")
            return
        self.__get_files_glob()
      

    def __repr__(self):
        return f"GitDownloader({self.repo_link=},{self.local_dir=})"