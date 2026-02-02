1️⃣ Dimension Customers — gold.dim_customers
🎯 Rôle

Contient les informations descriptives des clients.
Elle permet d’analyser les ventes par client, pays, genre, etc.

📐 Structure détaillée
Column Name	Type	Description
customer_key (PK)	INT	Surrogate key générée dans le DWH
customer_id	INT	Identifiant métier venant du CRM
customer_number	VARCHAR	Clé business (cst_key)
firstname	VARCHAR	Prénom nettoyé
lastname	VARCHAR	Nom nettoyé
marital_status	VARCHAR	Single / Married / n/a
gender	VARCHAR	Female / Male / n/a
create_date	DATE	Date de création du client
birthdate	DATE	Date de naissance (ERP)
country	VARCHAR	Pays standardisé
🧠 Logique importante

customer_key est une clé surrogate (clé technique).
→ Toujours utiliser des clés numériques dans un Data Warehouse.

Les données viennent de plusieurs sources :

CRM → infos principales

ERP → birthdate

Location table → country

On applique :

nettoyage (TRIM)

standardisation (CASE WHEN)

gestion des NULL

2️⃣ Dimension Products — gold.dim_products
🎯 Rôle

Contient les attributs descriptifs des produits.

Permet d’analyser les ventes par :

catégorie

sous-catégorie

ligne produit

coût

📐 Structure détaillée
Column Name	Type	Description
product_key (PK)	INT	Surrogate key
product_id	INT	Identifiant technique produit
product_number	VARCHAR	Clé business produit
product_name	VARCHAR	Nom du produit
category_id	VARCHAR	ID dérivé du product key
category	VARCHAR	Catégorie produit
subcategory	VARCHAR	Sous-catégorie
maintenance	VARCHAR	Maintenance flag
cost	DECIMAL	Coût produit
product_line	VARCHAR	Mountain / Road / Touring / etc
start_date	DATE	Début validité (SCD2)
end_date	DATE	Fin validité (SCD2)
🧠 Logique importante

Cette dimension implémente un SCD Type 2 (Slowly Changing Dimension Type 2).

Ça veut dire :

Quand un produit change (prix, ligne, etc)
→ On ne modifie pas l’ancienne ligne
→ On insère une nouvelle ligne
→ On met à jour end_date

Pourquoi ?

Parce qu’en analyse historique, on veut savoir :
“Quel était le coût du produit au moment de la vente ?”

Sans SCD2, tu perds l’historique.
Et perdre l’historique, en data warehouse, c’est un péché capital.

3️⃣ Fact Table — gold.fact_sales
🎯 Rôle

Table centrale du modèle.
Contient les mesures numériques.

Elle relie :

clients

produits

dates

📐 Structure détaillée
Column Name	Type	Description
order_number	VARCHAR	Numéro de commande
product_key	INT (FK)	Référence dim_products
customer_key	INT (FK)	Référence dim_customers
order_date	DATE	Date commande
shipping_date	DATE	Date expédition
due_date	DATE	Date échéance
sales_amount	DECIMAL	Montant total
quantity	INT	Quantité
sls_price	DECIMAL	Prix unitaire
🧠 Logique importante

Pas de texte inutile.

Seulement des clés + des métriques.

Table volumineuse.

Optimisée pour l’agrégation.

Exemple d’analyse possible :

Total sales by country

Revenue by product category

Sales evolution over time

Profit analysis

🎯 Pourquoi ce modèle est puissant ?

Parce qu’il respecte 3 principes fondamentaux :

Séparation des données descriptives (dimensions)

Centralisation des métriques (fact)

Utilisation de clés surrogate pour performance

C’est la base du décisionnel moderne.
<img width="671" height="300" alt="image" src="https://github.com/user-attachments/assets/de3dd043-2ec6-4dbf-a4a7-a936394584c8" />

