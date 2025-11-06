#!/usr/bin/env python3
"""
Script de conversion de géodatabase ESRI (.gdb) vers Spatialite.

Ce script convertit une géodatabase ESRI en base de données Spatialite
en préservant toutes les couches, géométries et attributs.
Amélioré avec logs détaillés, progression et multi-threading.

Author: Simon Bédard
Contact: software@servicesforestiers.tech
License: Unlicense (Public Domain)
"""

import argparse
import html
import logging
import os
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Optional, Tuple, Dict, List

try:
    from osgeo import gdal, ogr
    gdal.UseExceptions()
except ImportError:
    print("ERREUR: GDAL n'est pas installé. Installez-le avec: pip install gdal")
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None
    print("AVERTISSEMENT: tqdm n'est pas installé. Installez-le avec: pip install tqdm pour la barre de progression")


class ProcessMonitor:
    """
    Gestionnaire de monitoring de processus.
    
    Encapsule la logique de monitoring d'un processus subprocess avec :
    - Lecture de sortie dans un thread séparé
    - Monitoring périodique avec timeouts
    - Affichage des messages de statut
    - Détection de blocage
    
    Respecte le principe de responsabilité unique : monitoring uniquement.
    """
    
    def __init__(self, process: subprocess.Popen, logger: 'ProgressLogger', 
                 layer_name: str, no_output_timeout: int = 60, 
                 status_interval: int = 30):
        """
        Initialise le monitor.
        
        Args:
            process: Processus à monitorer
            logger: Logger pour les messages
            layer_name: Nom de la couche (pour les logs)
            no_output_timeout: Timeout en secondes sans sortie avant message debug
            status_interval: Intervalle en secondes entre messages de statut
        """
        self.process = process
        self.logger = logger
        self.layer_name = layer_name
        self.no_output_timeout = no_output_timeout
        self.status_interval = status_interval
        
        self.output_lines = []
        self.output_lock = threading.Lock()
        self.reading_done = threading.Event()
        self.reader_thread = None
    
    def _read_output(self):
        """Lit la sortie dans un thread séparé pour éviter les blocages."""
        try:
            for line in self.process.stdout:
                line = line.strip()
                if line:
                    with self.output_lock:
                        self.output_lines.append(line)
                    # Filtrer les messages de progression
                    if 'ERROR' in line.upper():
                        self.logger.error(f"[{self.layer_name}] {line}")
                    elif 'WARNING' in line.upper():
                        self.logger.warning(f"[{self.layer_name}] {line}")
                    else:
                        self.logger.debug(f"[{self.layer_name}] {line}")
        except Exception as e:
            self.logger.debug(f"[{self.layer_name}] Erreur lors de la lecture: {e}")
        finally:
            self.reading_done.set()
    
    def start_monitoring(self):
        """Démarre le monitoring du processus."""
        self.reader_thread = threading.Thread(target=self._read_output, daemon=True)
        self.reader_thread.start()
    
    def wait_for_completion(self) -> Tuple[list, int]:
        """
        Attend la fin du processus avec monitoring.
        
        Returns:
            Tuple (output_lines, return_code)
        """
        start_time = time.time()
        last_output_count = 0
        last_output_time = time.time()
        last_status_time = time.time()
        
        # Polling adaptatif : commencer avec 0.1s, augmenter progressivement
        poll_interval = 0.1  # 100ms pour réactivité
        max_poll_interval = 1.0  # Max 1 seconde
        stable_count = 0  # Compteur de stabilité (pas de nouveau output)
        
        while self.process.poll() is None:
            time.sleep(poll_interval)
            current_time = time.time()
            
            # Vérifier si on a reçu de nouvelles lignes
            with self.output_lock:
                current_line_count = len(self.output_lines)
                has_new_output = current_line_count > last_output_count
            
            if has_new_output:
                last_output_count = current_line_count
                last_output_time = current_time
                stable_count = 0
                # Réduire l'intervalle de polling si on a du nouveau output
                poll_interval = max(0.1, poll_interval * 0.9)
            else:
                stable_count += 1
                # Augmenter progressivement l'intervalle si stable
                if stable_count > 10:  # Après 10 vérifications sans nouveau output
                    poll_interval = min(max_poll_interval, poll_interval * 1.1)
            
            # Afficher un message de statut périodiquement
            elapsed = current_time - start_time
            if current_time - last_status_time >= self.status_interval:
                if self.process.poll() is None:
                    elapsed_str = self.logger.format_time(elapsed)
                    self.logger.info(
                        f"[{self.layer_name}] Conversion en cours... ({elapsed_str} écoulé, "
                        f"{current_line_count} messages reçus)"
                    )
                    last_status_time = current_time
            
            # Vérifier si le processus semble bloqué
            elapsed_since_output = current_time - last_output_time
            if elapsed_since_output > self.no_output_timeout:
                if self.process.poll() is None:
                    elapsed_str = self.logger.format_time(elapsed)
                    self.logger.debug(
                        f"[{self.layer_name}] Traitement silencieux en cours ({elapsed_str} écoulé)..."
                    )
                    last_output_time = current_time
        
        # Attendre que la lecture soit terminée (max 10 secondes)
        self.reading_done.wait(timeout=10.0)
        
        # Attendre le thread de lecture
        if self.reader_thread:
            self.reader_thread.join(timeout=2.0)
        
        with self.output_lock:
            return (self.output_lines.copy(), self.process.returncode)


class GDBMetadataExtractor:
    """
    Extrait les métadonnées depuis une géodatabase ESRI.
    
    Responsabilité unique : extraction des métadonnées (alias, domaines, clés primaires, triggers).
    """
    
    def __init__(self, gdb_path: Path, logger: 'ProgressLogger'):
        """
        Initialise l'extracteur.
        
        Args:
            gdb_path: Chemin vers la géodatabase
            logger: Logger pour les messages
        """
        self.gdb_path = gdb_path
        self.logger = logger
        self._catalog_cache = None  # Cache pour le fichier catalogue trouvé
        self._domains_cache = None  # Cache pour les domaines chargés
        self._datasource_cache = None  # Cache pour la datasource GDAL (réutilisation)
    
    def _get_datasource(self):
        """
        Obtient ou crée la datasource GDAL avec cache pour réutilisation.
        
        Returns:
            Datasource OGR ou None
        """
        if self._datasource_cache is None:
            try:
                driver = ogr.GetDriverByName("OpenFileGDB")
                self._datasource_cache = driver.Open(str(self.gdb_path), 0)
            except Exception as e:
                self.logger.debug(f"Erreur lors de l'ouverture de la datasource: {e}")
                return None
        return self._datasource_cache
    
    def extract_field_aliases(self, layer_name: str) -> Dict[str, str]:
        """
        Extrait les alias de champs pour une couche.
        
        Args:
            layer_name: Nom de la couche
            
        Returns:
            Dictionnaire {nom_champ: alias}
        """
        aliases = {}
        try:
            datasource = self._get_datasource()
            if datasource is None:
                return aliases
            
            layer = datasource.GetLayerByName(layer_name)
            if layer is None:
                return aliases
            
            layer_def = layer.GetLayerDefn()
            for i in range(layer_def.GetFieldCount()):
                field_def = layer_def.GetFieldDefn(i)
                field_name = field_def.GetName()
                
                # Essayer plusieurs méthodes pour obtenir l'alias
                alias = None
                
                # Méthode 1 : GetAlternativeNameRef() (si disponible dans GDAL 3.0+)
                try:
                    if hasattr(field_def, 'GetAlternativeNameRef'):
                        alias = field_def.GetAlternativeNameRef()
                except:
                    pass
                
                # Méthode 2 : Métadonnées de la couche
                if not alias:
                    try:
                        metadata = layer.GetMetadata()
                        if metadata:
                            # Essayer différentes clés possibles
                            possible_keys = [
                                f"FIELD_{i}_ALIAS",
                                f"{field_name}_ALIAS",
                                f"ALIAS_{field_name}",
                                f"FIELD_ALIAS_{i}"
                            ]
                            for key in possible_keys:
                                if key in metadata:
                                    alias = metadata[key]
                                    break
                    except:
                        pass
                
                # Méthode 3 : Métadonnées du datasource
                if not alias:
                    try:
                        ds_metadata = datasource.GetMetadata()
                        if ds_metadata:
                            possible_keys = [
                                f"{layer_name}.{field_name}.ALIAS",
                                f"{layer_name}.FIELD_{i}.ALIAS",
                                f"GDB_{layer_name}.{field_name}.ALIAS"
                            ]
                            for key in possible_keys:
                                if key in ds_metadata:
                                    alias = ds_metadata[key]
                                    break
                    except:
                        pass
                
                # Méthode 4 : Métadonnées du champ directement
                if not alias:
                    try:
                        if hasattr(field_def, 'GetMetadata'):
                            field_metadata = field_def.GetMetadata()
                            if field_metadata:
                                if 'ALIAS' in field_metadata:
                                    alias = field_metadata['ALIAS']
                                elif 'ALTERNATIVE_NAME' in field_metadata:
                                    alias = field_metadata['ALTERNATIVE_NAME']
                    except:
                        pass
                
                if alias and alias != field_name:
                    aliases[field_name] = alias
                    self.logger.debug(f"  Alias trouvé pour {field_name}: {alias}")
            
            # Ne pas fermer la datasource (réutilisation)
        except Exception as e:
            self.logger.debug(f"Erreur lors de l'extraction des alias pour {layer_name}: {e}")
        
        return aliases
    
    def extract_domain_values(self, layer_name: str) -> Dict[str, Dict[int, str]]:
        """
        Extrait les domaines codés (alias de valeurs) pour une couche.
        
        Args:
            layer_name: Nom de la couche
            
        Returns:
            Dictionnaire {nom_champ: {code: description}}
        """
        domains = {}
        try:
            datasource = self._get_datasource()
            if datasource is None:
                return domains
            
            layer = datasource.GetLayerByName(layer_name)
            if layer is None:
                return domains
            
            layer_def = layer.GetLayerDefn()
            
            # Charger tous les domaines depuis le fichier catalogue (une seule fois)
            all_domains = self._load_domains_from_catalog()
            
            for i in range(layer_def.GetFieldCount()):
                field_def = layer_def.GetFieldDefn(i)
                field_name = field_def.GetName()
                
                # Essayer d'extraire le domaine codé
                domain_values = {}
                
                try:
                    # Méthode 1 : GetDomainName() et lookup dans le catalogue XML
                    domain_name = None
                    if hasattr(field_def, 'GetDomainName'):
                        domain_name = field_def.GetDomainName()
                    
                    if domain_name:
                        # Méthode 1 : Extraire depuis le fichier catalogue XML (prioritaire)
                        if domain_name in all_domains:
                            domain_values = all_domains[domain_name].copy()
                            self.logger.debug(
                                f"  Domaine {domain_name} trouvé dans le catalogue: "
                                f"{len(domain_values)} valeurs avec descriptions"
                            )
                        
                        # Méthode 2 : Chercher dans les métadonnées de la couche
                        if not domain_values:
                            domain_values = self._extract_from_layer_metadata(
                                layer, field_name, domain_name
                            )
                        
                        # Méthode 3 : Chercher dans les métadonnées du datasource
                        if not domain_values:
                            domain_values = self._extract_from_datasource_metadata(
                                datasource, layer_name, field_name, domain_name
                            )
                        
                        # Méthode 4 : Chercher dans les métadonnées du champ
                        if not domain_values and hasattr(field_def, 'GetMetadata'):
                            domain_values = self._extract_from_field_metadata(field_def)
                        
                        # Méthode 5 : Extraire les valeurs uniques depuis les données
                        # (sans descriptions, mais on peut au moins avoir les codes)
                        if not domain_values:
                            domain_values = self._extract_unique_values_from_data(
                                layer, field_name, field_def
                            )
                    
                except Exception as e:
                    self.logger.debug(f"Erreur extraction domaine pour {field_name}: {e}")
                    pass
                
                if domain_values:
                    domains[field_name] = domain_values
                    self.logger.info(f"  ✓ Domaine trouvé pour {field_name}: {len(domain_values)} valeurs")
            
            # Ne pas fermer la datasource (réutilisation)
        except Exception as e:
            self.logger.debug(f"Erreur lors de l'extraction des domaines pour {layer_name}: {e}")
        
        return domains
    
    def _load_domains_from_catalog(self) -> Dict[str, Dict[int, str]]:
        """
        Charge tous les domaines depuis le fichier catalogue XML de la géodatabase.
        
        Selon l'architecture ESRI, les métadonnées (domaines, schémas, etc.) sont stockées
        dans les tables système (GDB_Items, etc.). Pour FileGDB, ces tables sont persistées
        dans les fichiers .gdbtable sous forme XML.
        
        Référence : https://pro.arcgis.com/en/pro-app/latest/help/data/geodatabases/overview/the-architecture-of-a-geodatabase.htm
        
        Utilise une découverte automatique du fichier catalogue et un parsing robuste.
        Met en cache le résultat pour éviter de re-scanner les fichiers.
        
        Returns:
            Dictionnaire {nom_domaine: {code: description}}
        """
        # Utiliser le cache si disponible
        if self._domains_cache is not None:
            return self._domains_cache
        
        all_domains = {}
        
        try:
            # Découvrir automatiquement le fichier catalogue (avec cache)
            if self._catalog_cache is None:
                catalog_file = self._find_domain_catalog_file()
                self._catalog_cache = catalog_file
            else:
                catalog_file = self._catalog_cache
            
            if not catalog_file:
                self.logger.debug("Aucun fichier catalogue de domaines trouvé")
                self._domains_cache = {}  # Mettre en cache même si vide
                return all_domains
            
            self.logger.debug(f"Fichier catalogue trouvé: {catalog_file.name}")
            
            # Lire le fichier
            try:
                with open(catalog_file, 'rb') as f:
                    data = f.read()
            except IOError as e:
                self.logger.debug(f"Erreur lors de la lecture du fichier catalogue: {e}")
                self._domains_cache = {}  # Cache vide en cas d'erreur
                return all_domains
            
            # Décoder en texte (UTF-8 avec tolérance pour les erreurs)
            try:
                text = data.decode('utf-8', errors='ignore')
            except Exception as e:
                self.logger.debug(f"Erreur lors du décodage du fichier: {e}")
                self._domains_cache = {}  # Cache vide en cas d'erreur
                return all_domains
            
            # Parser les domaines depuis le XML
            all_domains = self._parse_domains_from_xml(text)
            
            # Mettre en cache le résultat
            self._domains_cache = all_domains
            
            if all_domains:
                self.logger.info(
                    f"✓ {len(all_domains)} domaine(s) chargé(s) depuis le catalogue XML "
                    f"({catalog_file.name})"
                )
        
        except Exception as e:
            self.logger.debug(f"Erreur lors du chargement des domaines depuis le catalogue: {e}")
            self._domains_cache = {}  # Cache vide en cas d'erreur
        
        return all_domains
    
    def _find_domain_catalog_file(self) -> Optional[Path]:
        """
        Trouve automatiquement le fichier contenant les définitions de domaines.
        
        Selon l'architecture ESRI (https://pro.arcgis.com/en/pro-app/latest/help/data/geodatabases/overview/the-architecture-of-a-geodatabase.htm),
        les métadonnées sont stockées dans les tables système GDB_Items.
        Pour FileGDB, ces tables sont dans les fichiers .gdbtable.
        
        Stratégie :
        1. Chercher tous les fichiers .gdbtable dans la géodatabase
        2. Scanner chaque fichier pour détecter la présence de domaines XML
        3. Retourner le fichier le plus probable (celui avec le plus de domaines)
        
        Returns:
            Chemin vers le fichier catalogue ou None si aucun trouvé
        """
        if not self.gdb_path.exists() or not self.gdb_path.is_dir():
            return None
        
        # Chercher tous les fichiers .gdbtable
        gdbtable_files = list(self.gdb_path.glob("*.gdbtable"))
        
        if not gdbtable_files:
            return None
        
        # Scanner les fichiers pour trouver celui contenant des domaines
        best_file = None
        best_score = 0
        
        for gdbtable_file in sorted(gdbtable_files):
            try:
                # Vérifier si le fichier contient des domaines
                contains_domains, score = self._file_contains_domains(gdbtable_file)
                
                if contains_domains and score > best_score:
                    best_file = gdbtable_file
                    best_score = score
                    self.logger.debug(
                        f"Fichier candidat trouvé: {gdbtable_file.name} "
                        f"(score: {score})"
                    )
            except Exception as e:
                self.logger.debug(
                    f"Erreur lors de l'analyse de {gdbtable_file.name}: {e}"
                )
                continue
        
        return best_file
    
    def _file_contains_domains(self, file_path: Path) -> Tuple[bool, int]:
        """
        Vérifie si un fichier contient des définitions de domaines.
        
        Les domaines sont stockés dans les tables système GDB_Items sous forme XML
        dans les fichiers .gdbtable. Cette méthode détecte la présence de ces définitions
        en cherchant des patterns XML spécifiques aux domaines codés.
        
        Args:
            file_path: Chemin vers le fichier .gdbtable
            
        Returns:
            Tuple (contains_domains, score) où score est le nombre de domaines trouvés
        """
        if not file_path.exists():
            return (False, 0)
        
        try:
            # Lire un échantillon du fichier (premiers 1MB pour éviter de tout charger)
            with open(file_path, 'rb') as f:
                sample = f.read(1024 * 1024)  # 1MB
            
            # Décoder en texte
            text = sample.decode('utf-8', errors='ignore')
            
            # Chercher des indicateurs de domaines codés
            indicators = [
                ('GPCodedValueDomain2', 10),  # Tag principal des domaines codés
                ('<DomainName>', 5),          # Nom de domaine
                ('<CodedValue', 3),           # Valeur codée
                ('<Code>', 2),                # Code
                ('<Name>', 2),                # Description
            ]
            
            score = 0
            for indicator, points in indicators:
                if indicator in text:
                    score += points
            
            # Chercher aussi des patterns de domaines connus
            import re
            domain_pattern = r'<GPCodedValueDomain2[^>]*>.*?</GPCodedValueDomain2>'
            domain_matches = re.findall(domain_pattern, text, re.DOTALL)
            score += len(domain_matches) * 5  # Bonus pour chaque domaine trouvé
            
            # Si le fichier est petit, lire tout pour vérifier
            if file_path.stat().st_size < 10 * 1024 * 1024:  # < 10MB
                with open(file_path, 'rb') as f:
                    full_data = f.read()
                full_text = full_data.decode('utf-8', errors='ignore')
                full_domain_matches = re.findall(domain_pattern, full_text, re.DOTALL)
                score = len(full_domain_matches) * 5
            
            return (score > 20, score)  # Seuil minimal pour considérer comme valide
        
        except Exception as e:
            self.logger.debug(f"Erreur lors de la vérification de {file_path.name}: {e}")
            return (False, 0)
    
    def _parse_domains_from_xml(self, xml_content: str) -> Dict[str, Dict[int, str]]:
        """
        Parse le contenu XML pour extraire les domaines codés.
        
        Les domaines codés sont stockés dans des blocs <GPCodedValueDomain2> contenant :
        - <DomainName> : Nom du domaine
        - <CodedValues> : Liste de <CodedValue> avec <Code> et <Name>
        
        Gère :
        - Différents formats XML (avec/sans namespaces, variantes de tags)
        - Erreurs de parsing avec fallback gracieux
        - Formats de codes variés (numériques, textuels)
        - Encodages HTML dans les descriptions (&apos;, &quot;, etc.)
        
        Args:
            xml_content: Contenu XML à parser (extrait depuis les fichiers .gdbtable)
            
        Returns:
            Dictionnaire {nom_domaine: {code: description}}
        """
        all_domains = {}
        import re
        
        try:
            # Pattern pour trouver tous les domaines codés
            # Supporte les variantes avec/sans namespaces
            domain_patterns = [
                r'<GPCodedValueDomain2[^>]*>.*?</GPCodedValueDomain2>',
                r'<GPCodedValueDomain[^>]*>.*?</GPCodedValueDomain>',
            ]
            
            domain_matches = []
            for pattern in domain_patterns:
                matches = re.findall(pattern, xml_content, re.DOTALL | re.IGNORECASE)
                domain_matches.extend(matches)
            
            if not domain_matches:
                return all_domains
            
            # Parser chaque domaine
            for domain_xml in domain_matches:
                try:
                    domain_name, coded_values = self._parse_single_domain(domain_xml)
                    if domain_name and coded_values:
                        all_domains[domain_name] = coded_values
                        self.logger.debug(
                            f"Domaine parsé: {domain_name} ({len(coded_values)} valeurs)"
                        )
                except Exception as e:
                    self.logger.debug(f"Erreur lors du parsing d'un domaine: {e}")
                    continue
        
        except Exception as e:
            self.logger.debug(f"Erreur lors du parsing XML: {e}")
        
        return all_domains
    
    def _parse_single_domain(self, domain_xml: str) -> Tuple[Optional[str], Dict[int, str]]:
        """
        Parse un seul domaine XML.
        
        Args:
            domain_xml: Bloc XML <GPCodedValueDomain2>...</GPCodedValueDomain2>
            
        Returns:
            Tuple (nom_domaine, {code: description})
        """
        import re
        
        # Extraire le nom du domaine
        name_patterns = [
            r'<DomainName>([^<]+)</DomainName>',
            r'<DomainName[^>]*>([^<]+)</DomainName>',
        ]
        
        domain_name = None
        for pattern in name_patterns:
            match = re.search(pattern, domain_xml, re.IGNORECASE)
            if match:
                domain_name = match.group(1).strip()
                break
        
        if not domain_name:
            return (None, {})
        
        # Extraire les valeurs codées
        coded_values = {}
        coded_pattern = r'<CodedValue[^>]*>.*?</CodedValue>'
        coded_matches = re.findall(coded_pattern, domain_xml, re.DOTALL | re.IGNORECASE)
        
        for coded_xml in coded_matches:
            result = self._extract_coded_value(coded_xml)
            if result:
                code, description = result
                coded_values[code] = description
        
        return (domain_name, coded_values)
    
    def _extract_coded_value(self, coded_xml: str) -> Optional[Tuple[int, str]]:
        """
        Extrait un code et sa description depuis un bloc CodedValue.
        
        Gère :
        - Codes numériques (int)
        - Codes textuels (char, string)
        - Formats XML variés
        - Encodages spéciaux (&apos;, &quot;, etc.)
        
        Args:
            coded_xml: Bloc XML <CodedValue>...</CodedValue>
            
        Returns:
            Tuple (code, description) ou None si erreur
        """
        import re
        
        try:
            # Patterns pour extraire le Code
            code_patterns = [
                r'<Code[^>]*>([^<]+)</Code>',
                r'<Code[^>]*xsi:type=[\'"]xs:string[\'"]>([^<]+)</Code>',
                r'<Code[^>]*xsi:type=[\'"]xs:int[\'"]>([^<]+)</Code>',
            ]
            
            code_str = None
            for pattern in code_patterns:
                match = re.search(pattern, coded_xml, re.IGNORECASE)
                if match:
                    code_str = match.group(1).strip()
                    break
            
            # Patterns pour extraire le Name (description)
            name_patterns = [
                r'<Name>([^<]+)</Name>',
                r'<Name[^>]*>([^<]+)</Name>',
            ]
            
            description = None
            for pattern in name_patterns:
                match = re.search(pattern, coded_xml, re.IGNORECASE)
                if match:
                    description = match.group(1).strip()
                    break
            
            if not code_str or not description:
                return None
            
            # Décoder les entités HTML
            try:
                description = html.unescape(description)
            except:
                pass
            
            # Convertir le code en int si possible
            code = self._convert_code_to_int(code_str)
            
            return (code, description)
        
        except Exception as e:
            self.logger.debug(f"Erreur lors de l'extraction d'une valeur codée: {e}")
            return None
    
    def _convert_code_to_int(self, code_str: str) -> int:
        """
        Convertit un code (string) en entier pour le stockage.
        
        Gère :
        - Codes numériques : conversion directe
        - Codes textuels d'un caractère : ord()
        - Codes textuels multiples : hash
        
        Args:
            code_str: Code sous forme de string
            
        Returns:
            Code sous forme d'entier
        """
        # Essayer de convertir en int directement
        try:
            return int(code_str)
        except ValueError:
            pass
        
        # Pour les codes textuels d'un seul caractère, utiliser ord()
        if len(code_str) == 1:
            return ord(code_str)
        
        # Pour les codes textuels multiples, utiliser un hash déterministe
        # (hash() peut varier entre les sessions Python, utiliser abs() et modulo)
        return abs(hash(code_str)) % 1000000
    
    def _find_domain_tables(self, datasource) -> list:
        """
        Trouve les tables de domaine dans la géodatabase.
        
        Args:
            datasource: Datasource OGR
            
        Returns:
            Liste des noms de tables de domaine
        """
        domain_tables = []
        try:
            # Les tables de domaine ESRI ont souvent des noms comme "GDB_DomainValues"
            # ou sont dans des couches système
            layer_count = datasource.GetLayerCount()
            for i in range(layer_count):
                layer = datasource.GetLayerByIndex(i)
                layer_name = layer.GetName()
                # Chercher les tables qui pourraient contenir des domaines
                if 'domain' in layer_name.lower() or 'gdb' in layer_name.lower():
                    domain_tables.append(layer_name)
        except:
            pass
        return domain_tables
    
    def _extract_from_domain_table(self, datasource, domain_name: str, domain_tables: list) -> Dict[int, str]:
        """
        Extrait les valeurs d'un domaine depuis une table de domaine.
        
        Args:
            datasource: Datasource OGR
            domain_name: Nom du domaine
            domain_tables: Liste des tables de domaine possibles
            
        Returns:
            Dictionnaire {code: description}
        """
        domain_values = {}
        try:
            # Essayer de trouver la table de domaine correspondante
            for table_name in domain_tables:
                try:
                    domain_layer = datasource.GetLayerByName(table_name)
                    if domain_layer:
                        # Parcourir les entités pour trouver celles du domaine
                        domain_layer.ResetReading()
                        feature_def = domain_layer.GetLayerDefn()
                        for feature in domain_layer:
                            # Chercher les champs qui pourraient contenir le nom du domaine
                            feat_domain_name = None
                            code = None
                            desc = None
                            
                            for j in range(feature_def.GetFieldCount()):
                                field_def = feature_def.GetFieldDefn(j)
                                field_name = field_def.GetName().lower()
                                
                                # Chercher le nom du domaine
                                if 'domain' in field_name or 'name' in field_name:
                                    feat_domain_name = feature.GetFieldAsString(j)
                                
                                # Chercher le code
                                if 'code' in field_name:
                                    code = feature.GetFieldAsInteger(j)
                                
                                # Chercher la description
                                if 'desc' in field_name or 'value' in field_name or 'name' in field_name:
                                    desc = feature.GetFieldAsString(j)
                            
                            # Si le domaine correspond et qu'on a code et description
                            if feat_domain_name and domain_name.upper() in feat_domain_name.upper():
                                if code is not None and desc:
                                    domain_values[code] = desc
                except:
                    continue
        except:
            pass
        return domain_values
    
    def _extract_from_layer_metadata(self, layer, field_name: str, domain_name: Optional[str]) -> Dict[int, str]:
        """Extrait les valeurs de domaine depuis les métadonnées de la couche."""
        domain_values = {}
        try:
            metadata = layer.GetMetadata()
            if metadata:
                # Chercher des clés contenant le nom du champ ou domaine
                search_terms = [field_name]
                if domain_name:
                    search_terms.append(domain_name)
                
                for key, value in metadata.items():
                    key_upper = key.upper()
                    if any(term.upper() in key_upper for term in search_terms):
                        if 'CODE' in key_upper or 'DOMAIN' in key_upper or 'VALUE' in key_upper:
                            # Parser les valeurs
                            domain_values.update(self._parse_domain_string(value))
        except:
            pass
        return domain_values
    
    def _extract_from_datasource_metadata(self, datasource, layer_name: str, 
                                         field_name: str, domain_name: Optional[str]) -> Dict[int, str]:
        """Extrait les valeurs de domaine depuis les métadonnées du datasource."""
        domain_values = {}
        try:
            ds_metadata = datasource.GetMetadata()
            if ds_metadata:
                search_terms = [layer_name, field_name]
                if domain_name:
                    search_terms.append(domain_name)
                
                for key, value in ds_metadata.items():
                    key_upper = key.upper()
                    if any(term.upper() in key_upper for term in search_terms):
                        if 'CODE' in key_upper or 'DOMAIN' in key_upper or 'VALUE' in key_upper:
                            domain_values.update(self._parse_domain_string(value))
        except:
            pass
        return domain_values
    
    def _extract_from_field_metadata(self, field_def) -> Dict[int, str]:
        """Extrait les valeurs de domaine depuis les métadonnées du champ."""
        domain_values = {}
        try:
            field_metadata = field_def.GetMetadata()
            if field_metadata:
                for key, value in field_metadata.items():
                    if 'DOMAIN' in key.upper() or 'CODE' in key.upper() or 'VALUE' in key.upper():
                        domain_values.update(self._parse_domain_string(value))
        except:
            pass
        return domain_values
    
    def _parse_domain_string(self, value: str) -> Dict[int, str]:
        """
        Parse une chaîne contenant des valeurs de domaine.
        
        Formats supportés:
        - "CODE1:Description1;CODE2:Description2"
        - "1=Description1,2=Description2"
        - JSON-like formats
        
        Args:
            value: Chaîne à parser
            
        Returns:
            Dictionnaire {code: description}
        """
        domain_values = {}
        if not value:
            return domain_values
        
        try:
            # Format 1: "CODE1:Description1;CODE2:Description2"
            if ':' in value and ';' in value:
                parts = value.split(';')
                for part in parts:
                    if ':' in part:
                        code_str, desc = part.split(':', 1)
                        try:
                            code = int(code_str.strip())
                            domain_values[code] = desc.strip()
                        except ValueError:
                            pass
            
            # Format 2: "1=Description1,2=Description2"
            elif '=' in value and ',' in value:
                parts = value.split(',')
                for part in parts:
                    if '=' in part:
                        code_str, desc = part.split('=', 1)
                        try:
                            code = int(code_str.strip())
                            domain_values[code] = desc.strip()
                        except ValueError:
                            pass
            
            # Format 3: Simple "CODE:Description"
            elif ':' in value:
                code_str, desc = value.split(':', 1)
                try:
                    code = int(code_str.strip())
                    domain_values[code] = desc.strip()
                except ValueError:
                    pass
        except:
            pass
        
        return domain_values
    
    def _extract_unique_values_from_data(self, layer, field_name: str, field_def) -> Dict[int, str]:
        """
        Extrait les valeurs uniques depuis les données réelles.
        
        Note: Cette méthode ne peut pas obtenir les descriptions des valeurs,
        mais peut identifier les codes uniques utilisés.
        
        Args:
            layer: Layer OGR
            field_name: Nom du champ
            field_def: FieldDefn OGR
            
        Returns:
            Dictionnaire {code: ''} (descriptions vides car non accessibles)
        """
        domain_values = {}
        try:
            field_idx = layer.GetLayerDefn().GetFieldIndex(field_name)
            if field_idx < 0:
                return domain_values
            
            # Extraire les valeurs uniques (limité à 1000 pour performance)
            values = set()
            layer.ResetReading()
            count = 0
            for feature in layer:
                if count >= 10000:  # Limiter pour éviter les problèmes de performance
                    break
                value = feature.GetFieldAsString(field_idx)
                if value and value.strip():
                    values.add(value.strip())
                count += 1
            
            # Convertir en dictionnaire
            # Pour les valeurs numériques, utiliser directement comme code
            # Pour les valeurs textuelles, utiliser la valeur comme description avec un code dérivé
            for value in sorted(values):
                try:
                    # Essayer de convertir en int pour les codes numériques
                    code = int(value)
                    domain_values[code] = f"Valeur {value}"  # Description générique
                except ValueError:
                    # Pour les valeurs textuelles (ex: 'B', 'I', 'T'), créer un code basé sur la valeur
                    # Utiliser ord() pour créer un code unique à partir du caractère
                    if len(value) == 1:
                        code = ord(value)  # Utiliser le code ASCII comme code
                        domain_values[code] = value  # La valeur elle-même comme description
                    else:
                        # Pour les valeurs multiples, utiliser un hash
                        code = abs(hash(value)) % 1000000
                        domain_values[code] = value  # Utiliser la valeur comme description
            
            if domain_values:
                self.logger.debug(
                    f"  Valeurs uniques extraites pour {field_name}: {len(domain_values)} "
                    f"(descriptions non disponibles via GDAL)"
                )
        except Exception as e:
            self.logger.debug(f"Erreur extraction valeurs uniques pour {field_name}: {e}")
        
        return domain_values
    
    def extract_primary_keys(self, layer_name: str) -> List[str]:
        """
        Extrait les clés primaires pour une couche.
        
        Args:
            layer_name: Nom de la couche
            
        Returns:
            Liste des noms de champs formant la clé primaire
        """
        primary_keys = []
        try:
            datasource = self._get_datasource()
            if datasource is None:
                return primary_keys
            
            layer = datasource.GetLayerByName(layer_name)
            if layer is None:
                return primary_keys
            
            # Méthode 1 : FID column (toujours présent)
            fid_column = layer.GetFIDColumn()
            if fid_column:
                primary_keys.append(fid_column)
                self.logger.debug(f"  Clé primaire FID trouvée: {fid_column}")
            
            # Méthode 2 : Chercher dans les métadonnées
            try:
                metadata = layer.GetMetadata()
                if metadata:
                    # Chercher les clés primaires dans les métadonnées
                    for key in metadata.keys():
                        if "PRIMARY_KEY" in key.upper() or "PK_" in key.upper():
                            # Extraire les noms de champs
                            value = metadata[key]
                            if value:
                                # Format peut varier, essayer de parser
                                fields = [f.strip() for f in value.split(',')]
                                for field in fields:
                                    if field and field not in primary_keys:
                                        primary_keys.append(field)
            except:
                pass
            
            # Ne pas fermer la datasource (réutilisation)
        except Exception as e:
            self.logger.debug(f"Erreur lors de l'extraction des clés primaires pour {layer_name}: {e}")
        
        return primary_keys if primary_keys else ['ogc_fid']  # Fallback par défaut
    
    def extract_triggers(self, layer_name: str) -> List[str]:
        """
        Extrait les triggers pour une couche.
        
        Note: Les triggers ESRI ne sont généralement pas accessibles via GDAL/OGR.
        
        Args:
            layer_name: Nom de la couche
            
        Returns:
            Liste des définitions SQL de triggers (vide si non accessible)
        """
        triggers = []
        try:
            # Les triggers ESRI ne sont généralement pas accessibles via l'API GDAL
            # Cette méthode est un placeholder pour une future implémentation
            # qui pourrait utiliser des outils ESRI spécifiques
            
            self.logger.debug(f"Extraction des triggers non supportée pour {layer_name} (limitation GDAL)")
        except Exception as e:
            self.logger.debug(f"Erreur lors de l'extraction des triggers pour {layer_name}: {e}")
        
        return triggers
    
    def extract_all_metadata(self, layer_name: str) -> Dict:
        """
        Extrait toutes les métadonnées pour une couche.
        
        Args:
            layer_name: Nom de la couche
            
        Returns:
            Dictionnaire avec toutes les métadonnées
        """
        return {
            'field_aliases': self.extract_field_aliases(layer_name),
            'domain_values': self.extract_domain_values(layer_name),
            'primary_keys': self.extract_primary_keys(layer_name),
            'triggers': self.extract_triggers(layer_name)
        }


class SpatialiteMetadataApplier:
    """
    Applique les métadonnées dans une base Spatialite.
    
    Responsabilité unique : application des métadonnées via SQL.
    """
    
    def __init__(self, output_path: Path, logger: 'ProgressLogger'):
        """
        Initialise l'applicateur.
        
        Args:
            output_path: Chemin vers le fichier Spatialite
            logger: Logger pour les messages
        """
        self.output_path = output_path
        self.logger = logger
    
    def _execute_sql(self, sql: str) -> bool:
        """
        Exécute une requête SQL dans Spatialite.
        
        Args:
            sql: Requête SQL à exécuter
            
        Returns:
            True si succès, False sinon
        """
        try:
            conn = sqlite3.connect(str(self.output_path))
            conn.enable_load_extension(True)
            
            # Charger l'extension spatialite si disponible
            try:
                conn.execute("SELECT load_extension('mod_spatialite')")
            except:
                pass  # Si mod_spatialite n'est pas disponible, continuer
            
            conn.execute(sql)
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            self.logger.debug(f"Erreur SQL: {e}")
            self.logger.debug(f"Requête: {sql[:200]}")
            return False
    
    def apply_field_aliases(self, table_name: str, aliases: Dict[str, str]) -> bool:
        """
        Applique les alias de champs via une table de métadonnées.
        
        Args:
            table_name: Nom de la table
            aliases: Dictionnaire {nom_champ: alias}
            
        Returns:
            True si succès
        """
        if not aliases:
            return True
        
        try:
            # Créer la table de métadonnées si elle n'existe pas
            self._execute_sql("""
                CREATE TABLE IF NOT EXISTS metadata_field_aliases (
                    table_name TEXT NOT NULL,
                    field_name TEXT NOT NULL,
                    alias TEXT NOT NULL,
                    PRIMARY KEY (table_name, field_name)
                )
            """)
            
            # Insérer les alias
            conn = sqlite3.connect(str(self.output_path))
            cursor = conn.cursor()
            
            for field_name, alias in aliases.items():
                cursor.execute("""
                    INSERT OR REPLACE INTO metadata_field_aliases 
                    (table_name, field_name, alias) 
                    VALUES (?, ?, ?)
                """, (table_name, field_name, alias))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"✓ {len(aliases)} alias de champs appliqués pour '{table_name}'")
            return True
        except Exception as e:
            self.logger.warning(f"Erreur lors de l'application des alias pour {table_name}: {e}")
            return False
    
    def apply_domain_values(self, table_name: str, domain_values: Dict[str, Dict[int, str]]) -> bool:
        """
        Applique les domaines codés via une table de correspondance.
        
        Args:
            table_name: Nom de la table
            domain_values: Dictionnaire {nom_champ: {code: description}}
            
        Returns:
            True si succès
        """
        # Toujours créer la table même si vide, pour la cohérence
        try:
            self._execute_sql("""
                CREATE TABLE IF NOT EXISTS metadata_domain_values (
                    table_name TEXT NOT NULL,
                    field_name TEXT NOT NULL,
                    code INTEGER NOT NULL,
                    description TEXT NOT NULL,
                    PRIMARY KEY (table_name, field_name, code)
                )
            """)
        except Exception as e:
            self.logger.debug(f"Erreur lors de la création de metadata_domain_values: {e}")
        
        if not domain_values:
            return True
        
        try:
            
            # Insérer les valeurs de domaine
            conn = sqlite3.connect(str(self.output_path))
            cursor = conn.cursor()
            
            total_values = 0
            for field_name, values in domain_values.items():
                for code, description in values.items():
                    cursor.execute("""
                        INSERT OR REPLACE INTO metadata_domain_values 
                        (table_name, field_name, code, description) 
                        VALUES (?, ?, ?, ?)
                    """, (table_name, field_name, code, description))
                    total_values += 1
            
            conn.commit()
            conn.close()
            
            self.logger.info(
                f"✓ {total_values} valeurs de domaine appliquées pour '{table_name}' "
                f"({len(domain_values)} champs)"
            )
            return True
        except Exception as e:
            self.logger.warning(f"Erreur lors de l'application des domaines pour {table_name}: {e}")
            return False
    
    def apply_primary_keys(self, table_name: str, primary_keys: List[str]) -> bool:
        """
        Applique les clés primaires.
        
        Args:
            table_name: Nom de la table
            primary_keys: Liste des champs formant la clé primaire
            
        Returns:
            True si succès
        """
        if not primary_keys:
            return True
        
        try:
            # Vérifier si la clé primaire existe déjà
            conn = sqlite3.connect(str(self.output_path))
            cursor = conn.cursor()
            
            # Vérifier si la table existe
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))
            
            if not cursor.fetchone():
                conn.close()
                return False
            
            # Créer un index unique pour simuler la clé primaire
            # (si ce n'est pas déjà le champ ogc_fid qui est géré automatiquement)
            if len(primary_keys) == 1 and primary_keys[0] in ['ogc_fid', 'fid', 'OBJECTID']:
                # La clé primaire est déjà gérée par Spatialite
                conn.close()
                self.logger.debug(f"Clé primaire {primary_keys[0]} déjà gérée pour '{table_name}'")
                return True
            
            # Créer un index unique composé
            index_name = f"pk_{table_name}_{'_'.join(primary_keys)}"
            index_name = index_name.replace('-', '_').replace('.', '_')[:50]  # Limiter la longueur
            
            # Construire la requête SQL
            columns = ', '.join([f'"{col}"' for col in primary_keys])
            sql = f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {index_name} 
                ON "{table_name}" ({columns})
            """
            
            success = self._execute_sql(sql)
            conn.close()
            
            if success:
                self.logger.info(f"✓ Clé primaire appliquée pour '{table_name}': {', '.join(primary_keys)}")
            return success
        except Exception as e:
            self.logger.warning(f"Erreur lors de l'application de la clé primaire pour {table_name}: {e}")
            return False
    
    def apply_triggers(self, table_name: str, triggers: List[str]) -> bool:
        """
        Applique les triggers.
        
        Args:
            table_name: Nom de la table
            triggers: Liste des définitions SQL de triggers
            
        Returns:
            True si succès
        """
        if not triggers:
            return True
        
        success_count = 0
        for trigger_sql in triggers:
            if self._execute_sql(trigger_sql):
                success_count += 1
        
        if success_count > 0:
            self.logger.info(f"✓ {success_count} trigger(s) appliqué(s) pour '{table_name}'")
        
        return success_count == len(triggers)
    
    def apply_all_metadata(self, table_name: str, metadata: Dict) -> bool:
        """
        Applique toutes les métadonnées pour une table.
        
        Args:
            table_name: Nom de la table
            metadata: Dictionnaire avec toutes les métadonnées
            
        Returns:
            True si toutes les applications réussissent
        """
        success = True
        success &= self.apply_field_aliases(table_name, metadata.get('field_aliases', {}))
        success &= self.apply_domain_values(table_name, metadata.get('domain_values', {}))
        success &= self.apply_primary_keys(table_name, metadata.get('primary_keys', []))
        success &= self.apply_triggers(table_name, metadata.get('triggers', []))
        return success


class ProgressLogger:
    """
    Gestionnaire de logs et progression.
    Respecte le principe de responsabilité unique : gestion des logs uniquement.
    """
    
    def __init__(self, verbose: bool = True):
        """
        Initialise le logger.
        
        Args:
            verbose: Si True, affiche les logs détaillés
        """
        self.verbose = verbose
        self.logger = logging.getLogger('GDB2SQL')
        self.logger.setLevel(logging.DEBUG if verbose else logging.INFO)
        
        # Handler pour la console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
        
        # Format des logs
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        self.progress_lock = Lock()
        self.start_time = None
    
    def info(self, message: str):
        """Affiche un message d'information."""
        self.logger.info(message)
    
    def debug(self, message: str):
        """Affiche un message de debug."""
        self.logger.debug(message)
    
    def warning(self, message: str):
        """Affiche un avertissement."""
        self.logger.warning(message)
    
    def error(self, message: str):
        """Affiche une erreur."""
        self.logger.error(message)
    
    def start_timer(self):
        """Démarre le chronomètre."""
        self.start_time = time.time()
    
    def get_elapsed_time(self) -> float:
        """Retourne le temps écoulé en secondes."""
        if self.start_time:
            return time.time() - self.start_time
        return 0.0
    
    def format_time(self, seconds: float) -> str:
        """Formate le temps en chaîne lisible."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"


class GDBToSpatialiteConverter:
    """
    Convertisseur de géodatabase ESRI vers Spatialite.
    
    Respecte le principe de responsabilité unique : une seule classe
    pour une seule fonctionnalité (la conversion).
    """
    
    def __init__(self, gdb_path: str, output_path: str, logger: Optional[ProgressLogger] = None):
        """
        Initialise le convertisseur.
        
        Args:
            gdb_path: Chemin vers le dossier .gdb
            output_path: Chemin vers le fichier Spatialite de sortie
            logger: Instance de logger (optionnel)
        """
        self.gdb_path = Path(gdb_path)
        self.output_path = Path(output_path)
        self.logger = logger or ProgressLogger()
        self._validate_inputs()
        
        # Initialiser les extracteurs/applicateurs de métadonnées
        self.metadata_extractor = GDBMetadataExtractor(self.gdb_path, self.logger)
        self.metadata_applier = SpatialiteMetadataApplier(self.output_path, self.logger)
    
    def _validate_inputs(self) -> None:
        """
        Valide les chemins d'entrée et de sortie.
        
        Raises:
            ValueError: Si les chemins ne sont pas valides
        """
        self.logger.debug(f"Validation des chemins: GDB={self.gdb_path}, Output={self.output_path}")
        
        if not self.gdb_path.exists():
            raise ValueError(f"Le chemin GDB n'existe pas: {self.gdb_path}")
        
        if not self.gdb_path.is_dir():
            raise ValueError(f"Le chemin GDB doit être un dossier: {self.gdb_path}")
        
        # S'assurer que le répertoire de sortie existe
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger.debug(f"Répertoire de sortie vérifié: {self.output_path.parent}")
        
        # Vérifier que l'extension est .sqlite ou .db
        if self.output_path.suffix not in ['.sqlite', '.db']:
            self.logger.warning(f"L'extension {self.output_path.suffix} n'est pas standard pour Spatialite.")
            self.logger.warning("Extension recommandée: .sqlite ou .db")
    
    def get_layers(self) -> list:
        """
        Récupère la liste des couches dans la géodatabase.
        
        Returns:
            Liste des noms de couches
        """
        self.logger.info(f"Ouverture de la géodatabase: {self.gdb_path}")
        driver = ogr.GetDriverByName("OpenFileGDB")
        if driver is None:
            raise RuntimeError("Le driver OpenFileGDB n'est pas disponible. Vérifiez l'installation de GDAL.")
        
        datasource = driver.Open(str(self.gdb_path), 0)
        if datasource is None:
            raise RuntimeError(f"Impossible d'ouvrir la géodatabase: {self.gdb_path}")
        
        layers = []
        layer_count = datasource.GetLayerCount()
        self.logger.info(f"Nombre de couches trouvées: {layer_count}")
        
        for i in range(layer_count):
            layer = datasource.GetLayerByIndex(i)
            layer_name = layer.GetName()
            feature_count = layer.GetFeatureCount()
            layers.append(layer_name)
            self.logger.debug(f"  Couche {i+1}: {layer_name} ({feature_count} entités)")
        
        datasource = None
        return layers
    
    def _get_layer_info(self, layer_name: str) -> dict:
        """
        Récupère les informations sur une couche.
        
        Args:
            layer_name: Nom de la couche
            
        Returns:
            Dictionnaire avec les informations de la couche
        """
        driver = ogr.GetDriverByName("OpenFileGDB")
        datasource = driver.Open(str(self.gdb_path), 0)
        if datasource is None:
            return {}
        
        layer = datasource.GetLayerByName(layer_name)
        if layer is None:
            datasource = None
            return {}
        
        info = {
            'name': layer_name,
            'feature_count': layer.GetFeatureCount(),
            'geom_type': layer.GetGeomType(),
            'srs': layer.GetSpatialRef(),
            'field_count': layer.GetLayerDefn().GetFieldCount()
        }
        
        datasource = None
        return info
    
    @contextmanager
    def _progress_bar_context(self, total: int, desc: str = "Conversion"):
        """
        Context manager pour la barre de progression tqdm.
        
        Args:
            total: Nombre total d'éléments
            desc: Description de la barre
            
        Yields:
            Barre de progression ou None si tqdm n'est pas disponible
        """
        progress_bar = None
        if tqdm:
            progress_bar = tqdm(total=total, desc=desc, unit="couche")
        try:
            yield progress_bar
        finally:
            if progress_bar:
                progress_bar.close()
    
    def _display_conversion_summary(self, success_count: int, total_count: int, 
                                   failed_layers: list, output_path: Path):
        """
        Affiche le résumé de la conversion.
        
        Args:
            success_count: Nombre de couches converties avec succès
            total_count: Nombre total de couches
            failed_layers: Liste des couches échouées [(nom, erreur), ...]
            output_path: Chemin vers le fichier de sortie
        """
        elapsed = self.logger.get_elapsed_time()
        self.logger.info("=" * 60)
        self.logger.info("Résumé de la conversion")
        self.logger.info("=" * 60)
        self.logger.info(f"Temps total: {self.logger.format_time(elapsed)}")
        self.logger.info(f"Couches réussies: {success_count}/{total_count}")
        
        if failed_layers:
            self.logger.warning(f"Couches échouées: {len(failed_layers)}")
            for layer, error in failed_layers:
                self.logger.warning(f"  - {layer}: {error[:100]}")
        
        if success_count > 0:
            file_size = output_path.stat().st_size / (1024 * 1024)  # MB
            self.logger.info(f"✓ Fichier créé: {output_path} ({file_size:.2f} MB)")
        else:
            self.logger.error("✗ Aucune couche n'a été convertie")
            if output_path.exists():
                output_path.unlink()
    
    def _build_ogr2ogr_command(self, layer_name: str, overwrite: bool, is_first: bool, 
                                fast_mode: bool = False) -> list:
        """
        Construit la commande ogr2ogr pour convertir une couche.
        
        Args:
            layer_name: Nom de la couche à convertir
            overwrite: Si True, écrase le fichier de sortie
            is_first: Si True, c'est la première couche (création du fichier)
            fast_mode: Si True, active les optimisations agressives (sécurité réduite)
            
        Returns:
            Liste des arguments de la commande ogr2ogr
        """
        cmd = [
            'ogr2ogr',
            '-f', 'SQLite',
            '-dsco', 'SPATIALITE=YES',
            '-progress',  # Afficher la progression
        ]
        
        # Options de performance SQLite (toujours activées)
        # Note: Les optimisations SQLite (PRAGMA) sont appliquées après conversion
        # via _optimize_spatialite_database() car ogr2ogr ne supporte pas ces options
        cmd.extend([
            '-dsco', 'INIT_WITH_EPSG=NO',  # Éviter la réinitialisation si déjà fait
        ])
        
        # Options de couche (index spatiaux MUST HAVE)
        cmd.extend([
            '-lco', 'SPATIAL_INDEX=YES',  # Créer l'index spatial immédiatement (MUST HAVE)
            '-lco', 'GEOMETRY_NAME=geometry',  # Nom standardisé
        ])
        
        if is_first and overwrite:
            cmd.extend(['-overwrite'])
        elif not is_first:
            cmd.extend(['-update'])
        
        # Pour OpenFileGDB, on spécifie la couche avec l'option -sql
        cmd.extend([
            str(self.output_path),
            str(self.gdb_path),
            '-sql', f'SELECT * FROM "{layer_name}"'
        ])
        
        return cmd
    
    def _convert_layer_with_ogr2ogr(self, layer_name: str, overwrite: bool, is_first: bool,
                                    fast_mode: bool = False) -> Tuple[str, bool, str]:
        """
        Convertit une couche en utilisant ogr2ogr.
        
        Args:
            layer_name: Nom de la couche à convertir
            overwrite: Si True, écrase le fichier de sortie
            is_first: Si True, c'est la première couche (création du fichier)
            fast_mode: Si True, active les optimisations agressives
            
        Returns:
            Tuple (layer_name, success, error_message)
        """
        try:
            # Construire la commande ogr2ogr
            cmd = self._build_ogr2ogr_command(layer_name, overwrite, is_first, fast_mode)
            self.logger.debug(f"Commande ogr2ogr: {' '.join(cmd)}")
            
            # Lancer la conversion avec capture de la progression
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Utiliser ProcessMonitor pour gérer le monitoring
            monitor = ProcessMonitor(process, self.logger, layer_name)
            monitor.start_monitoring()
            output_lines, return_code = monitor.wait_for_completion()
            
            if return_code == 0:
                self.logger.info(f"✓ Couche '{layer_name}' convertie avec succès")
                return (layer_name, True, "")
            else:
                error_msg = "\n".join(output_lines[-10:])  # Dernières 10 lignes
                self.logger.error(f"✗ Erreur lors de la conversion de '{layer_name}': {error_msg}")
                return (layer_name, False, error_msg)
                
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"✗ Exception lors de la conversion de '{layer_name}': {error_msg}")
            return (layer_name, False, error_msg)
    
    def _prepare_conversion(self, layer_name: Optional[str], overwrite: bool, 
                           use_ogr2ogr: bool, max_workers: int) -> dict:
        """
        Prépare la conversion en validant et récupérant les informations nécessaires.
        
        Args:
            layer_name: Nom de la couche à convertir (None pour toutes)
            overwrite: Si True, écrase le fichier de sortie
            use_ogr2ogr: Si True, utilise ogr2ogr
            max_workers: Nombre de threads demandé
            
        Returns:
            Dictionnaire avec: {'layers': list, 'use_ogr2ogr': bool, 'max_workers': int}
        """
        self.logger.start_timer()
        self.logger.info("=" * 60)
        self.logger.info("Début de la conversion GDB vers Spatialite")
        self.logger.info("=" * 60)
        self.logger.info(f"Source: {self.gdb_path}")
        self.logger.info(f"Destination: {self.output_path}")
        
        if self.output_path.exists() and not overwrite:
            raise FileExistsError(
                f"Le fichier de sortie existe déjà: {self.output_path}\n"
                "Utilisez --overwrite pour l'écraser."
            )
        
        # Vérifier ogr2ogr si demandé
        if use_ogr2ogr:
            try:
                result = subprocess.run(['ogr2ogr', '--version'], 
                                       capture_output=True, check=True)
                ogr_version = result.stdout.decode().strip().split('\n')[0]
                self.logger.info(f"ogr2ogr disponible: {ogr_version}")
            except (subprocess.CalledProcessError, FileNotFoundError):
                self.logger.warning("ogr2ogr n'est pas disponible, utilisation de l'API Python GDAL...")
                use_ogr2ogr = False
        
        # Obtenir la liste des couches à convertir
        all_layers = self.get_layers()
        layers_to_convert = [layer_name] if layer_name else all_layers
        
        if not layers_to_convert:
            self.logger.error("Aucune couche à convertir")
            return {'layers': [], 'use_ogr2ogr': False, 'max_workers': 1}
        
        self.logger.info(f"Couches à convertir: {len(layers_to_convert)}")
        for i, layer in enumerate(layers_to_convert, 1):
            info = self._get_layer_info(layer)
            if info:
                self.logger.info(f"  {i}. {layer}: {info.get('feature_count', '?')} entités")
        
        # Supprimer le fichier existant si overwrite est activé
        if self.output_path.exists() and overwrite:
            self.logger.info(f"Suppression du fichier existant: {self.output_path}")
            self.output_path.unlink()
        
        # Détecter si plusieurs couches vont vers un seul fichier
        # SQLite ne supporte pas bien l'accès concurrent multi-processus
        if len(layers_to_convert) > 1 and max_workers > 1:
            self.logger.warning("=" * 60)
            self.logger.warning("ATTENTION: Parallélisation désactivée")
            self.logger.warning("=" * 60)
            self.logger.warning(
                "Plusieurs couches sont converties vers un seul fichier SQLite.\n"
                "SQLite ne supporte pas l'accès concurrent en écriture depuis\n"
                "plusieurs processus. La conversion sera effectuée séquentiellement\n"
                "pour garantir la fiabilité."
            )
            self.logger.warning("=" * 60)
            max_workers = 1
        
        return {
            'layers': layers_to_convert,
            'use_ogr2ogr': use_ogr2ogr,
            'max_workers': max_workers
        }
    
    def _get_spatialite_table_name(self, layer_name: str) -> Optional[str]:
        """
        Récupère le nom réel de la table dans Spatialite.
        
        Args:
            layer_name: Nom de la couche dans la géodatabase
            
        Returns:
            Nom de la table dans Spatialite ou None si introuvable
        """
        if not self.output_path.exists():
            return None
        
        try:
            conn = sqlite3.connect(str(self.output_path))
            cursor = conn.cursor()
            
            # Chercher la table (peut être en minuscules, majuscules, ou tel quel)
            # Essayer différentes variantes
            possible_names = [
                layer_name,
                layer_name.lower(),
                layer_name.upper(),
                layer_name.replace('-', '_'),
                layer_name.replace(' ', '_')
            ]
            
            for name in possible_names:
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name=?
                """, (name,))
                result = cursor.fetchone()
                if result:
                    conn.close()
                    return result[0]
            
            conn.close()
            return None
        except Exception as e:
            self.logger.debug(f"Erreur lors de la recherche de la table {layer_name}: {e}")
            return None
    
    def _post_process_metadata(self, layer_name: str, 
                               preserve_aliases: bool = True,
                               preserve_domains: bool = True,
                               preserve_primary_keys: bool = True,
                               preserve_triggers: bool = True) -> bool:
        """
        Applique les métadonnées après conversion d'une couche.
        
        Args:
            layer_name: Nom de la couche convertie
            preserve_aliases: Si True, préserve les alias de champs
            preserve_domains: Si True, préserve les domaines codés
            preserve_primary_keys: Si True, préserve les clés primaires
            preserve_triggers: Si True, préserve les triggers
            
        Returns:
            True si toutes les métadonnées sont appliquées avec succès
        """
        if not self.output_path.exists():
            return False
        
        try:
            self.logger.info(f"Application des métadonnées pour '{layer_name}'...")
            
            # Extraire les métadonnées
            metadata = self.metadata_extractor.extract_all_metadata(layer_name)
            
            # Filtrer selon les options
            filtered_metadata = {}
            if preserve_aliases:
                filtered_metadata['field_aliases'] = metadata.get('field_aliases', {})
            if preserve_domains:
                filtered_metadata['domain_values'] = metadata.get('domain_values', {})
            if preserve_primary_keys:
                filtered_metadata['primary_keys'] = metadata.get('primary_keys', [])
            if preserve_triggers:
                filtered_metadata['triggers'] = metadata.get('triggers', [])
            
            # Appliquer les métadonnées
            # Vérifier le nom réel de la table dans Spatialite (peut être en minuscules)
            table_name = self._get_spatialite_table_name(layer_name)
            if table_name:
                success = self.metadata_applier.apply_all_metadata(table_name, filtered_metadata)
            else:
                self.logger.warning(f"Table '{layer_name}' introuvable dans Spatialite, métadonnées non appliquées")
                success = False
            
            if success:
                self.logger.info(f"✓ Métadonnées appliquées pour '{layer_name}'")
            else:
                self.logger.warning(f"⚠ Certaines métadonnées n'ont pas pu être appliquées pour '{layer_name}'")
            
            return success
        except Exception as e:
            self.logger.warning(f"Erreur lors de l'application des métadonnées pour {layer_name}: {e}")
            return False
    
    def _optimize_spatialite_database(self, fast_mode: bool = False):
        """
        Optimise les paramètres SQLite/Spatialite après conversion.
        
        Configure les paramètres de performance pour améliorer les opérations futures.
        Les index spatiaux sont déjà créés (MUST HAVE).
        
        Args:
            fast_mode: Si True, optimisations agressives (sécurité réduite)
        """
        if not self.output_path.exists():
            return
        
        try:
            conn = sqlite3.connect(str(self.output_path))
            
            # Activer l'extension spatialite si disponible
            try:
                conn.enable_load_extension(True)
                conn.execute("SELECT load_extension('mod_spatialite')")
            except:
                pass
            
            # Optimisations modérées (par défaut)
            pragmas = [
                "PRAGMA synchronous = NORMAL;",  # Au lieu de FULL
                "PRAGMA journal_mode = WAL;",   # Write-Ahead Logging (plus rapide)
                "PRAGMA cache_size = -256000;",  # 256MB de cache
                "PRAGMA temp_store = MEMORY;",   # Tables temporaires en mémoire
            ]
            
            # Optimisations agressives (fast_mode)
            if fast_mode:
                pragmas = [
                    "PRAGMA synchronous = OFF;",  # Désactiver complètement
                    "PRAGMA journal_mode = OFF;",  # Désactiver le journal
                    "PRAGMA cache_size = -512000;",  # 512MB de cache
                    "PRAGMA temp_store = MEMORY;",
                ]
                self.logger.info("Optimisations agressives activées (fast_mode)")
            
            for pragma in pragmas:
                try:
                    conn.execute(pragma)
                except Exception as e:
                    self.logger.debug(f"Erreur lors de l'exécution de {pragma}: {e}")
            
            # Analyser les statistiques pour optimiser les requêtes
            try:
                conn.execute("ANALYZE;")
            except:
                pass
            
            conn.commit()
            conn.close()
            
            self.logger.info("✓ Base SQLite optimisée pour les performances")
        
        except Exception as e:
            self.logger.debug(f"Erreur lors de l'optimisation de la base: {e}")
    
    def convert(self, layer_name: Optional[str] = None, overwrite: bool = False, 
                max_workers: int = 1, use_ogr2ogr: bool = True,
                preserve_metadata: bool = True,
                preserve_aliases: bool = True,
                preserve_domains: bool = True,
                preserve_primary_keys: bool = True,
                preserve_triggers: bool = True,
                fast_mode: bool = False) -> bool:
        """
        Convertit la géodatabase en Spatialite.
        
        Args:
            layer_name: Nom de la couche à convertir (None pour toutes les couches)
            overwrite: Si True, écrase le fichier de sortie s'il existe
            max_workers: Nombre de threads pour la conversion parallèle (1 = séquentiel)
            use_ogr2ogr: Si True, utilise ogr2ogr (plus fiable), sinon API Python
            preserve_metadata: Si True, préserve les métadonnées (alias, domaines, etc.)
            preserve_aliases: Si True, préserve les alias de champs
            preserve_domains: Si True, préserve les domaines codés
            preserve_primary_keys: Si True, préserve les clés primaires
            preserve_triggers: Si True, préserve les triggers
            fast_mode: Si True, active les optimisations agressives (sécurité réduite)
            
        Returns:
            True si la conversion réussit, False sinon
        """
        prep = self._prepare_conversion(layer_name, overwrite, use_ogr2ogr, max_workers)
        
        if not prep['layers']:
            return False
        
        # Conversion
        if prep['use_ogr2ogr']:
            return self._convert_with_ogr2ogr_parallel(
                prep['layers'], overwrite, prep['max_workers'],
                preserve_metadata, preserve_aliases, preserve_domains, 
                preserve_primary_keys, preserve_triggers, fast_mode
            )
        else:
            return self._convert_with_python_api(
                prep['layers'], overwrite,
                preserve_metadata, preserve_aliases, preserve_domains,
                preserve_primary_keys, preserve_triggers, fast_mode
            )
    
    def _convert_with_ogr2ogr_parallel(self, layers: list, overwrite: bool, max_workers: int,
                                      preserve_metadata: bool = True,
                                      preserve_aliases: bool = True,
                                      preserve_domains: bool = True,
                                      preserve_primary_keys: bool = True,
                                      preserve_triggers: bool = True,
                                      fast_mode: bool = False) -> bool:
        """
        Convertit les couches en utilisant ogr2ogr en parallèle.
        
        Args:
            layers: Liste des couches à convertir
            overwrite: Si True, écrase le fichier de sortie
            max_workers: Nombre de threads (sera forcé à 1 si plusieurs couches vers un seul fichier)
            
        Returns:
            True si au moins une conversion réussit
        """
        # Sécurité supplémentaire : vérifier si plusieurs couches vers un seul fichier
        if len(layers) > 1 and max_workers > 1:
            self.logger.debug(
                "Détection: plusieurs couches vers un seul fichier SQLite. "
                f"Forçage max_workers à 1 (était {max_workers})"
            )
            max_workers = 1
        
        self.logger.info(f"Conversion avec ogr2ogr (max_workers={max_workers})")
        
        success_count = 0
        failed_layers = []
        
        with self._progress_bar_context(len(layers)) as progress_bar:
            if max_workers == 1:
                # Conversion séquentielle avec progression
                for i, layer in enumerate(layers):
                    is_first = (i == 0)
                    self.logger.info(f"[{i+1}/{len(layers)}] Conversion de '{layer}'...")
                    
                    layer_name, success, error = self._convert_layer_with_ogr2ogr(
                        layer, overwrite, is_first, fast_mode
                    )
                    
                    if success:
                        success_count += 1
                        
                        # Appliquer les métadonnées après conversion réussie
                        if preserve_metadata:
                            self._post_process_metadata(
                                layer, preserve_aliases, preserve_domains,
                                preserve_primary_keys, preserve_triggers
                            )
                    else:
                        failed_layers.append((layer_name, error))
                    
                    if progress_bar:
                        progress_bar.update(1)
            else:
                # Conversion parallèle
                self.logger.info(f"Conversion parallèle avec {max_workers} workers")
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = []
                    
                    for i, layer in enumerate(layers):
                        is_first = (i == 0)
                        future = executor.submit(
                            self._convert_layer_with_ogr2ogr,
                            layer, overwrite, is_first, fast_mode
                        )
                        futures.append(future)
                    
                    # Traiter les résultats au fur et à mesure
                    for future in as_completed(futures):
                        layer_name, success, error = future.result()
                        
                        if success:
                            success_count += 1
                            
                            # Appliquer les métadonnées après conversion réussie
                            if preserve_metadata:
                                self._post_process_metadata(
                                    layer_name, preserve_aliases, preserve_domains,
                                    preserve_primary_keys, preserve_triggers
                                )
                        else:
                            failed_layers.append((layer_name, error))
                        
                        if progress_bar:
                            progress_bar.update(1)
        
        self._display_conversion_summary(success_count, len(layers), failed_layers, self.output_path)
        
        # Optimiser la base SQLite après conversion
        if success_count > 0:
            self._optimize_spatialite_database(fast_mode)
        
        return success_count > 0
    
    def _convert_with_python_api(self, layers: list, overwrite: bool,
                                preserve_metadata: bool = True,
                                preserve_aliases: bool = True,
                                preserve_domains: bool = True,
                                preserve_primary_keys: bool = True,
                                preserve_triggers: bool = True,
                                fast_mode: bool = False) -> bool:
        """
        Convertit les couches en utilisant l'API Python GDAL.
        
        Args:
            layers: Liste des couches à convertir
            overwrite: Si True, écrase le fichier de sortie
            
        Returns:
            True si au moins une conversion réussit
        """
        self.logger.info("Conversion avec l'API Python GDAL")
        
        driver_gdb = ogr.GetDriverByName("OpenFileGDB")
        driver_sqlite = ogr.GetDriverByName("SQLite")
        
        if driver_gdb is None:
            self.logger.error("Le driver OpenFileGDB n'est pas disponible.")
            return False
        
        if driver_sqlite is None:
            self.logger.error("Le driver SQLite n'est pas disponible.")
            return False
        
        # Ouvrir la source GDB
        source_ds = driver_gdb.Open(str(self.gdb_path), 0)
        if source_ds is None:
            self.logger.error(f"Impossible d'ouvrir la géodatabase: {self.gdb_path}")
            return False
        
        # Créer la destination Spatialite
        if self.output_path.exists():
            self.output_path.unlink()
        
        dest_ds = driver_sqlite.CreateDataSource(str(self.output_path))
        if dest_ds is None:
            self.logger.error(f"Impossible de créer le fichier Spatialite: {self.output_path}")
            source_ds = None
            return False
        
        # Initialiser Spatialite
        try:
            dest_ds.ExecuteSQL("SELECT InitSpatialMetadata(1)")
            self.logger.info("Spatialite initialisé")
        except Exception as e:
            self.logger.warning(f"Impossible d'initialiser Spatialite automatiquement: {e}")
        
        success_count = 0
        failed_layers = []
        
        with self._progress_bar_context(len(layers)) as progress_bar:
            for i, layer_name_item in enumerate(layers):
                self.logger.info(f"[{i+1}/{len(layers)}] Conversion de '{layer_name_item}'...")
                
                source_layer = source_ds.GetLayerByName(layer_name_item)
                if source_layer is None:
                    self.logger.warning(f"Couche '{layer_name_item}' introuvable, ignorée.")
                    failed_layers.append((layer_name_item, "Couche introuvable"))
                    if progress_bar:
                        progress_bar.update(1)
                    continue
                
                feature_count = source_layer.GetFeatureCount()
                self.logger.debug(f"  Entités à convertir: {feature_count}")
                
                # Créer la couche de destination
                dest_layer = dest_ds.CreateLayer(
                    layer_name_item,
                    source_layer.GetSpatialRef(),
                    source_layer.GetGeomType(),
                    ['SPATIAL_INDEX=YES', 'FORMAT=SPATIALITE']
                )
                
                if dest_layer is None:
                    self.logger.error(f"Échec de la création de la couche '{layer_name_item}'.")
                    failed_layers.append((layer_name_item, "Échec de création"))
                    if progress_bar:
                        progress_bar.update(1)
                    continue
                
                # Copier la définition des champs
                source_layer_def = source_layer.GetLayerDefn()
                field_count = source_layer_def.GetFieldCount()
                self.logger.debug(f"  Champs à copier: {field_count}")
                
                for j in range(field_count):
                    field_def = source_layer_def.GetFieldDefn(j)
                    dest_layer.CreateField(field_def)
                
                # Copier les entités avec progression
                source_layer.ResetReading()
                feature_bar = None
                if tqdm and feature_count > 100:
                    feature_bar = tqdm(total=feature_count, desc=f"  {layer_name_item}", 
                                      unit="entité", leave=False)
                
                converted = 0
                for feature in source_layer:
                    dest_layer.CreateFeature(feature)
                    converted += 1
                    if feature_bar and converted % 100 == 0:
                        feature_bar.update(100)
                
                if feature_bar:
                    feature_bar.update(feature_count - converted)
                    feature_bar.close()
                
                dest_layer.SyncToDisk()
                self.logger.info(f"✓ Couche '{layer_name_item}' convertie: {converted} entités")
                success_count += 1
                
                # Appliquer les métadonnées après conversion réussie
                if preserve_metadata:
                    self._post_process_metadata(
                        layer_name_item, preserve_aliases, preserve_domains,
                        preserve_primary_keys, preserve_triggers
                    )
                
                if progress_bar:
                    progress_bar.update(1)
        
        # Fermer les datasources
        source_ds = None
        dest_ds = None
        
        self._display_conversion_summary(success_count, len(layers), failed_layers, self.output_path)
        
        # Optimiser la base SQLite après conversion
        if success_count > 0:
            self._optimize_spatialite_database(fast_mode)
        
        return success_count > 0


def main():
    """
    Point d'entrée principal du script.
    
    Gère l'interface en ligne de commande et orchestre la conversion.
    """
    parser = argparse.ArgumentParser(
        description="Convertit une géodatabase ESRI (.gdb) en Spatialite (.sqlite)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  # Convertir toutes les couches
  python gdb_to_spatialite.py Role_2024.gdb output.sqlite
  
  # Convertir avec multi-threading
  python gdb_to_spatialite.py Role_2024.gdb output.sqlite --workers 4
  
  # Convertir une couche spécifique
  python gdb_to_spatialite.py Role_2024.gdb output.sqlite --layer "NomCouche"
  
  # Écraser le fichier de sortie s'il existe
  python gdb_to_spatialite.py Role_2024.gdb output.sqlite --overwrite
  
  # Lister les couches disponibles
  python gdb_to_spatialite.py Role_2024.gdb output.sqlite --list-layers
        """
    )
    
    parser.add_argument(
        "gdb_path",
        type=str,
        help="Chemin vers le dossier .gdb (géodatabase ESRI)"
    )
    
    parser.add_argument(
        "output_path",
        type=str,
        help="Chemin vers le fichier Spatialite de sortie (.sqlite ou .db)"
    )
    
    parser.add_argument(
        "--layer",
        type=str,
        default=None,
        help="Nom de la couche spécifique à convertir (par défaut: toutes les couches)"
    )
    
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Écraser le fichier de sortie s'il existe déjà"
    )
    
    parser.add_argument(
        "--list-layers",
        action="store_true",
        help="Lister les couches disponibles dans la géodatabase et quitter"
    )
    
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Nombre de threads pour la conversion parallèle (défaut: 1, séquentiel)"
    )
    
    parser.add_argument(
        "--no-ogr2ogr",
        action="store_true",
        help="Forcer l'utilisation de l'API Python au lieu d'ogr2ogr"
    )
    
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Mode silencieux (moins de logs)"
    )
    
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        help="Désactiver la préservation des métadonnées (alias, domaines, clés primaires, triggers)"
    )
    
    parser.add_argument(
        "--skip-aliases",
        action="store_true",
        help="Ne pas préserver les alias de champs"
    )
    
    parser.add_argument(
        "--skip-domains",
        action="store_true",
        help="Ne pas préserver les domaines codés (alias de valeurs)"
    )
    
    parser.add_argument(
        "--skip-primary-keys",
        action="store_true",
        help="Ne pas recréer les clés primaires"
    )
    
    parser.add_argument(
        "--skip-triggers",
        action="store_true",
        help="Ne pas recréer les triggers"
    )
    
    parser.add_argument(
        "--fast-mode",
        action="store_true",
        help="Activer les optimisations agressives (sécurité réduite, performance maximale)"
    )
    
    args = parser.parse_args()
    
    try:
        logger = ProgressLogger(verbose=not args.quiet)
        converter = GDBToSpatialiteConverter(args.gdb_path, args.output_path, logger)
        
        if args.list_layers:
            print(f"Couches disponibles dans {args.gdb_path}:")
            layers = converter.get_layers()
            for i, layer in enumerate(layers, 1):
                info = converter._get_layer_info(layer)
                feature_count = info.get('feature_count', '?') if info else '?'
                print(f"  {i}. {layer} ({feature_count} entités)")
            return 0
        
        success = converter.convert(
            layer_name=args.layer,
            overwrite=args.overwrite,
            max_workers=args.workers,
            use_ogr2ogr=not args.no_ogr2ogr,
            preserve_metadata=not args.no_metadata,
            preserve_aliases=not args.skip_aliases,
            preserve_domains=not args.skip_domains,
            preserve_primary_keys=not args.skip_primary_keys,
            preserve_triggers=not args.skip_triggers,
            fast_mode=args.fast_mode
        )
        return 0 if success else 1
    
    except ValueError as e:
        print(f"ERREUR: {e}")
        return 1
    except FileExistsError as e:
        print(f"ERREUR: {e}")
        return 1
    except Exception as e:
        print(f"ERREUR inattendue: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
