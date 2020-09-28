# Data Visualization course
This repository contains the code that loads the datasets and generates the different plots and figures used in the 
assignment of the Data and Results Visualization course from Politecnico di Milano. 

## Installation

Clone the repo using Github Desktop or by command line
```shell script
git clone https://github.com/fernandobperezm/data-and-results-visualization.git
```

From the command line, navigate and enter the repository folder and execute the following commands.
```shell script
conda env create -f environment.yml
conda activate data-visualization-course
python run_visualization.py
```

Install git-lfs
```shell script
brew install git-lfs
```

## Data Processing
After installing the environment, if you wish to rerun our calculations or include other cities, then you can use the `Data processing.ipynb` notebook.

## Results Visualization
We generated the figures and plots using a single notebook `Notebook for Visualization.ipynb`, in particular, we used Bokeh as the tool
that creates these plots. This decision was made after passing from matplotlib and seaborn and see that, for our case, 
these were not as interactive as we wanted. For instance, using Bokeh we can zoom-in a particular plot without needing to
write the code for that zoom.

We also used RISE, a library to transform Jupyter notebooks into slideshows for interactive teaching. In particular, our presentation
solely used RISE to show, detail, explain, and play with all the plots and figures.

The only limitation that we see of using Jupyter Notebooks + Bokeh + RISE is that if you have lots of visualizations (like we do 🤣),
then your browser could crash. 

## Report
In the repo, inside the `report` folder you'll find a copy of the LaTeX source code used to generate the report. You'll 
also find `Report.pdf`, a pdf file generated from compiling that source code.

## Authors
- Selva Calixto Guzmán: Ph.D. student in Energy, Nuclear Science and Technology at Politecnico di Milano.
- Fernando Benjamín Pérez Maurera: Ph.D. student in Information Technology at Politecnico di Milano.
