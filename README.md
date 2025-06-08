## Project Setup

This project uses `pyenv` and `pyenv-virtualenv` to manage
python virtual environments. To run the project locally,
you will need an environment called `sparkenv`.

Here is the way how to create it:
```bash
# install pyenv and pyenv-virtualenv first, then run:
pyenv virtualenv 3.12 sparkenv
pyenv shell sparkenv
pip install pyspark
```

Then, every time you will run Python commands in this repository, Python will automatically detect the
`.python-version` file and run code in the proper environment

The guide I was using personally:
[PySpark Installation Guide](https://sparkbyexamples.com/pyspark/how-to-install-pyspark-on-mac/)