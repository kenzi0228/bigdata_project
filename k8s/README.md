# k8s

Aucun manifeste Kubernetes n'a ete ecrit a la main : le pipeline tourne dans un service
**Vscode-python** lance depuis le catalogue Onyxia, qui deploie le chart Helm
`ide/vscode-python` (2.6.6) dans le namespace `user-m-lali-ece`.
Les ressources creees (relevees avec `kubectl`, voir evidence/01) sont resumees dans
`vscode-python-service.yaml` (valeurs utiles du chart, secrets retires).

Un essai avec le service **Jupyter-pyspark** (chart `ide/jupyter-pyspark` 3.2.7) a ete
abandonne : le pod est reste en attente ("0/9 nodes are available: Insufficient cpu").
