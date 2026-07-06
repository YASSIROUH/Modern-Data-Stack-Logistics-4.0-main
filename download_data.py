import kagglehub
import shutil
import glob
import os

print('Début du téléchargement du dataset Kaggle...')
path = kagglehub.dataset_download('shashwatwork/dataco-smart-supply-chain-for-big-data-analysis')
print(f'Fichier mis en cache dans : {path}')

csv_files = glob.glob(os.path.join(path, '*.csv'))
for f in csv_files:
    target = os.path.join('..', os.path.basename(f))
    shutil.copy(f, target)
    print(f'Copié avec succès -> {target}')

print('Opération terminée !')
