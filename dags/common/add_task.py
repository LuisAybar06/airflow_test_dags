from airflow.decorators import task, dag
import logging
from datetime import datetime, timedelta

@task
def sum_task(x, y):
    print(f"Task args: x={x}, y={y}")
    return x + y


@task.virtualenv(
    task_id="virtualenv_python", requirements=["pandas", "numpy", "scikit-learn"], system_site_packages=False
)
def task_virtualenv():
    import numpy as np
    from sklearn.model_selection import train_test_split
    import sys
    print("Python Version")
    print(sys.version)
    print("-----------")
    
    print("Init")
    x = np.arange(1, 25).reshape(12, 2)
    y = np.array([0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 1, 0])
    x_train, x_test, y_train, y_test = train_test_split(x, y)

    print('Train:')
    print(x_train)
    print('Test:')
    print(x_test)
    print("Finished")


@task.virtualenv(
    task_id="training_model", 
    requirements=[
        "pandas", 
        "numpy", 
        "scikit-learn",
        "joblib"
        ], 
    system_site_packages=False
)
def task_trainmodel():
    import sys
    from datetime import datetime
    from sklearn.linear_model import Lasso
    import pandas as pd
    import joblib

    PATH_COMMON = '../'
    sys.path.append(PATH_COMMON)

    X_train = pd.read_csv('/opt/airflow/dags/data/inputs/xtrain.csv') 
    y_train = pd.read_csv('/opt/airflow/dags/data/inputs/ytrain.csv') 

    # Seleccionar características
    features = pd.read_csv('/opt/airflow/dags/data/inputs/selected_features.csv')
    features = features['0'].to_list()

    X_train = X_train[features]

    # Configurar el modelo
    lin_model = Lasso(alpha=0.001, random_state=0)

    # Entrenar el modelo
    lin_model.fit(X_train, y_train)

    # Guardar el modelo
    joblib.dump(lin_model, '/opt/airflow/dags/data/model/linear_regression.joblib')

    print("Modelo Guardado")


@task.virtualenv(
    task_id="prediction_model", 
    requirements=[
        "pandas", 
        "numpy", 
        "scikit-learn",
        "joblib"
        ], 
    system_site_packages=False
)
def task_predictionnmodel():
    import sys
    from datetime import datetime
    from sklearn.linear_model import Lasso
    from joblib import load
    import pandas as pd
    import joblib

    PATH_COMMON = '../'
    sys.path.append(PATH_COMMON)

    X_train = pd.read_csv('/opt/airflow/dags/data/inputs/xtrain.csv')
    classifier = load("/opt/airflow/dags/data/model/linear_regression.joblib")

    # Seleccionar características
    features = pd.read_csv('/opt/airflow/dags/data/inputs/selected_features.csv')
    features = features['0'].to_list()
    X_train = X_train[features]


    #Realizamos la prediccion
    predictions = classifier.predict(X_train)

    predictions = pd.DataFrame(predictions, columns=['prediction'])
    #Guardamos el resultado
    predictions.to_csv('/opt/airflow/dags/data/outputs/predictions.csv', index=False)

    print("Prediccion Generada")



@task.virtualenv(
    task_id="monitoring_model", 
    requirements=[
        "pandas", 
        "numpy", 
        "scikit-learn",
        "joblib"
        ], 
    system_site_packages=False
)
def task_monitoringmodel():
    import pandas as pd
    import numpy as np
    from joblib import load
    from sklearn.metrics import mean_squared_error, r2_score
    from datetime import datetime
    import os

    # Cargar datos
    X_train = pd.read_csv('/opt/airflow/dags/data/inputs/xtrain.csv')
    y_train = pd.read_csv('/opt/airflow/dags/data/inputs/ytrain.csv')

    X_test = pd.read_csv('/opt/airflow/dags/data/inputs/xtest.csv')
    y_test = pd.read_csv('/opt/airflow/dags/data/inputs/ytest.csv')

    # Cargar el modelo
    lin_model = load("/opt/airflow/dags/data/model/linear_regression.joblib")

    # Cargar las características seleccionadas
    features = pd.read_csv('/opt/airflow/dags/data/inputs/selected_features.csv')
    features = features['0'].to_list()

    # Filtrar las características seleccionadas en los conjuntos de entrenamiento y prueba
    X_train = X_train[features]
    X_test = X_test[features]

    # Hacer predicciones en el conjunto de entrenamiento
    pred_train = lin_model.predict(X_train)

    # Calcular métricas para el conjunto de entrenamiento
    train_mse = mean_squared_error(np.exp(y_train), np.exp(pred_train))
    train_rmse = mean_squared_error(np.exp(y_train), np.exp(pred_train), squared=False)
    train_r2 = r2_score(np.exp(y_train), np.exp(pred_train))

    # Hacer predicciones en el conjunto de prueba
    pred_test = lin_model.predict(X_test)

    # Calcular métricas para el conjunto de prueba
    test_mse = mean_squared_error(np.exp(y_test), np.exp(pred_test))
    test_rmse = mean_squared_error(np.exp(y_test), np.exp(pred_test), squared=False)
    test_r2 = r2_score(np.exp(y_test), np.exp(pred_test))

    # Calcular el precio promedio de la casa
    avg_house_price = np.exp(y_train).median()

    # Obtener la importancia de las características
    importance = pd.Series(np.abs(lin_model.coef_.ravel()))
    importance.index = features
    importance.sort_values(inplace=True, ascending=False)

    # Crear una tabla con los resultados y la fecha de procesamiento
    processing_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Crear un DataFrame para las métricas y el precio promedio
    metrics_results = pd.DataFrame({
        'Processing Date': [processing_date],
        'Train MSE': [train_mse],
        'Train RMSE': [train_rmse],
        'Train R2': [train_r2],
        'Test MSE': [test_mse],
        'Test RMSE': [test_rmse],
        'Test R2': [test_r2],
        'Average House Price': [avg_house_price]
    })

    # Crear un DataFrame para la importancia de las características
    feature_importance = pd.DataFrame({
        'Processing Date': [processing_date] * len(features),
        'Feature': importance.index,
        'Importance': importance.values
    })

    # Rutas a los archivos CSV de salida
    metrics_results_path = '/opt/airflow/dags/data/outputs/model_evaluation_metrics_results.csv'
    feature_importance_path = '/opt/airflow/dags/data/outputs/model_evaluation_feature_importance.csv'

    # Guardar ambos DataFrames en archivos CSV, añadiendo si ya existen
    if os.path.isfile(metrics_results_path):
        metrics_results.to_csv(metrics_results_path, mode='a', header=False, index=False)
    else:
        metrics_results.to_csv(metrics_results_path, mode='w', header=True, index=False)

    if os.path.isfile(feature_importance_path):
        feature_importance.to_csv(feature_importance_path, mode='a', header=False, index=False)
    else:
        feature_importance.to_csv(feature_importance_path, mode='w', header=True, index=False)

    




