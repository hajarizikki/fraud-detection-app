# 🛡️ Fraud Detection App

Une application web de détection de fraude bancaire en ligne, construite avec **Flask**, **Apache Spark MLlib**, **Docker**, et **Jupyter Notebook**.

---

## 🎯 Objectif

Ce projet vise à prédire si une transaction est frauduleuse ou non à partir de ses caractéristiques, en exploitant un modèle **Random Forest** entraîné sur PySpark. L’interface permet à l’utilisateur de soumettre une transaction et d’obtenir une prédiction avec visualisation.

---

## 🚀 Fonctionnalités

- ✅ Interface intuitive pour saisir les détails d’une transaction
- 🧠 Prédiction en temps réel avec un modèle Spark Random Forest
- 📊 Visualisation des probabilités (transaction sûre vs fraude)
- 🎨 Design responsive avec CSS et icônes Font Awesome
- 🐳 Déploiement facile via Docker et Docker Compose

---

## 🗂️ Structure du projet

fraud-detect/
│

├── api/ # API Flask

│ ├── app.py # Serveur principal

│ ├── templates/ # HTML (formulaire et résultat)

│ └── static/css/ # Fichiers CSS (style, result)

│
├── notebooks/ # Explorations et entraînement Spark ML

│ └── RF_fraud_model # Modèle RandomForest enregistré
│


│
├── Dockerfile

├── docker-compose.yml

└── README.md

---

## 🛠️ Technologies utilisées

Python 3.9
Flask
PySpark (Spark MLlib)
Docker & Docker Compose
HTML5 / CSS3
Chart.js
Font Awesome

## ✅ Exemple de prédiction

| Entrée              | Valeur             |
| ------------------- | ------------------ |
| Type de transaction | `CASH_OUT`         |
| Step                | `1`                |
| Montant             | `10000 MAD`        |
| Ancien solde        | `19000 MAD`        |
| Nouveau solde       | `9000 MAD`         |
| Résultat            | ✅ Transaction sûre |



