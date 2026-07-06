"""
Logistics 4.0 Data Streaming Manager.
Responsible for orchestrating the injection of logistics data into the Kafka cluster.

Modes:
    sain   — Clean nominal data, no anomalies
    chaos  — 100%% Smart Mix anomalies (all events corrupted)
    mix    — Smart Mix: causally coherent anomalies at 50%% rate
    ia     — AI self-learning mode: generates edge-case events, adapts from errors
    aio    — All-In-One premium: 35%% nominal / 30%% AI boundary / 20%% Smart Mix / 15%% chaos
"""

import argparse
import json
import logging
import os
import random
import time
from collections import deque
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

import pandas as pd
from kafka import KafkaProducer
import logging_loki


# ==============================================================================
# CONFIGURATION — supports both HOST (localhost) and Docker (kafka:29092) contexts
# ==============================================================================
LOKI_URL = os.environ.get('LOKI_URL', 'http://localhost:3100/loki/api/v1/push')
KAFKA_BOOTSTRAP_SERVERS = [os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')]
KAFKA_TOPIC = 'dataco_orders'
DATASET_FILE_PATH = os.environ.get('DATASET_FILE_PATH', '../DataCoSupplyChainDataset.csv')
CURSOR_FILE = 'stream_cursor.txt'

# Configure Loki Emitter
logging_loki.emitter.LokiEmitter.level_tag = "level"
loki_handler = logging_loki.LokiHandler(
    url=LOKI_URL,
    tags={"application": "logistics_pipeline", "component": "streaming", "env": "production"},
    version="1",
)
logger = logging.getLogger("stream_manager")
logger.setLevel(logging.DEBUG)
logger.addHandler(loki_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


# ==============================================================================
# SMART MIX CORRUPTOR — Causally Coherent Anomalies
# ==============================================================================
class SmartMixCorruptor:
    """
    Generates business-coherent anomalies for pipeline resilience testing.

    Unlike pure random corruption, each anomaly type follows supply chain
    causal logic so the ML model can meaningfully learn from these edge cases.
    Anomaly distribution is weighted by real-world frequency.
    """

    # Realistic shipping risk profiles (delay probability per mode)
    SHIPPING_RISK = {
        'Same Day': 0.05,
        'First Class': 0.15,
        'Second Class': 0.45,
        'Standard Class': 0.65
    }

    # High-risk geographic regions (supply chain complexity)
    HIGH_RISK_REGIONS = [
        'Africa', 'South Asia', 'Southeast Asia',
        'Central America', 'West Africa', 'Eastern Africa'
    ]

    @classmethod
    def inject(cls, msg: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """
        Injects a causally coherent business anomaly into the event.

        Returns:
            (corrupted_msg, anomaly_type_label)
        """
        roll = random.random()

        if roll < 0.20:
            return cls._scheduling_paradox(msg)
        elif roll < 0.38:
            return cls._revenue_integrity(msg)
        elif roll < 0.52:
            return cls._geo_routing_failure(msg)
        elif roll < 0.66:
            return cls._demand_surge(msg)
        elif roll < 0.78:
            return cls._product_substitution(msg)
        elif roll < 0.88:
            return cls._null_field_corruption(msg)
        else:
            return cls._decision_boundary_case(msg)

    @classmethod
    def _scheduling_paradox(cls, msg: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """Shipping mode is fast but delivery window is impossibly long → guaranteed late."""
        msg['Shipping Mode'] = 'Standard Class'
        msg['Days for shipment (scheduled)'] = random.randint(6, 9)
        msg['Late_delivery_risk'] = 1
        msg['Delivery Status'] = 'Late delivery'
        logger.error(f"[ANOMALY:SCHEDULING_PARADOX] Order {msg.get('Order Id')} — "
                     f"Standard Class forced with {msg['Days for shipment (scheduled)']}d window")
        return msg, 'SCHEDULING_PARADOX'

    @classmethod
    def _revenue_integrity(cls, msg: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """Negative margins with excessive discounts — fraud/input error pattern."""
        original = msg.get('Order Item Total', 50.0) or 50.0
        factor = random.uniform(-0.6, -0.1)
        msg['Order Item Total'] = round(original * factor, 2)
        msg['Benefit per order'] = round(msg['Order Item Total'] * -0.3, 2)
        msg['Order Item Discount'] = round(abs(msg['Order Item Total']) * random.uniform(0.6, 0.9), 2)
        logger.error(f"[ANOMALY:REVENUE_INTEGRITY] Order {msg.get('Order Id')} — "
                     f"Negative total: {msg['Order Item Total']}")
        return msg, 'REVENUE_INTEGRITY'

    @classmethod
    def _geo_routing_failure(cls, msg: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """Order origin country doesn't match logistics delivery region → routing failure → late."""
        geo_mismatches = [
            ('United States', 'West Africa', 1),
            ('Mexico', 'Southeast Asia', 1),
            ('France', 'South America', 1),
            ('China', 'Central America', 1),
            ('Germany', 'Eastern Africa', 1),
        ]
        country, region, risk = random.choice(geo_mismatches)
        msg['Order Country'] = country
        msg['Order Region'] = region
        msg['Late_delivery_risk'] = risk
        logger.error(f"[ANOMALY:GEO_ROUTING_FAILURE] Order {msg.get('Order Id')} — "
                     f"{country} → {region} mismatch")
        return msg, 'GEO_ROUTING_FAILURE'

    @classmethod
    def _demand_surge(cls, msg: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """Sudden volume spike → logistics capacity overwhelmed → delays likely."""
        surge_factor = random.uniform(8, 15)
        msg['Order Item Total'] = round((msg.get('Order Item Total', 50) or 50) * surge_factor, 2)
        msg['Sales per customer'] = round((msg.get('Sales per customer', 100) or 100) * surge_factor * 0.7, 2)
        # Rush orders under surge = delayed
        if msg.get('Days for shipment (scheduled)', 3) <= 2:
            msg['Late_delivery_risk'] = 1
            msg['Delivery Status'] = 'Late delivery'
        logger.warning(f"[ANOMALY:DEMAND_SURGE] Order {msg.get('Order Id')} — "
                       f"Volume surge ×{surge_factor:.1f}")
        return msg, 'DEMAND_SURGE'

    @classmethod
    def _product_substitution(cls, msg: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """Out-of-stock forces product substitution → shipping mode override → added delay."""
        substitutions = [
            ('Fishing', 'Outdoors', 'Standard Class', 3),
            ('Electronics', 'Technology', 'Second Class', 2),
            ('Cleats', 'Sporting Goods', 'Standard Class', 4),
            ('Camping & Hiking', 'Outdoors', 'Second Class', 3),
        ]
        cat, dept, new_mode, extra_days = random.choice(substitutions)
        scheduled = msg.get('Days for shipment (scheduled)', 3) or 3
        msg['Category Name'] = cat
        msg['Department Name'] = dept
        msg['Shipping Mode'] = new_mode
        msg['Days for shipment (scheduled)'] = scheduled + extra_days
        msg['Late_delivery_risk'] = 1
        logger.warning(f"[ANOMALY:PRODUCT_SUBSTITUTION] Order {msg.get('Order Id')} — "
                       f"Substituted to {cat}, new mode: {new_mode}")
        return msg, 'PRODUCT_SUBSTITUTION'

    @classmethod
    def _null_field_corruption(cls, msg: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """Critical field nullification — tests pipeline null-handling & validation."""
        field = random.choice(['Order Id', 'Customer Id', 'Order Status', 'Product Price'])
        original = msg.get(field)
        msg[field] = None
        logger.error(f"[ANOMALY:NULL_FIELD] Order {msg.get('Order Id')} — "
                     f"Field '{field}' nullified (was: {original})")
        return msg, f'NULL_{field.upper().replace(" ", "_")}'

    @classmethod
    def _decision_boundary_case(cls, msg: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """
        Intentional edge case near the model's decision boundary.
        Mixed high-risk and low-risk signals to create maximum learning value.
        """
        # Ambiguous shipping mode (neither fast nor slow)
        msg['Shipping Mode'] = random.choice(['First Class', 'Second Class'])
        # Middle-ground delivery window (borderline risky)
        msg['Days for shipment (scheduled)'] = random.randint(3, 5)
        # Mixed geographic signal
        msg['Order Region'] = random.choice(['Western Europe', 'South Asia', 'US Center', 'Southeast Asia'])
        # Marginal benefit (borderline financial stress)
        msg['Benefit per order'] = round(random.uniform(-3, 3), 2)
        # Keep original Late_delivery_risk (don't override — keeps it authentic)
        logger.info(f"[ANOMALY:EDGE_CASE] Order {msg.get('Order Id')} — "
                    f"Boundary case: {msg['Shipping Mode']}, {msg['Days for shipment (scheduled)']}d")
        return msg, 'EDGE_CASE'


# ==============================================================================
# AI SELF-LEARNING MODE
# ==============================================================================
class AIFluxMode:
    """
    AI-driven flux generator with closed-loop self-learning.

    Architecture:
    1. For each event, predict Late_delivery_risk using business heuristic
    2. Compare prediction to actual label from dataset
    3. Track running error rate on a 200-event sliding window
    4. Adapt event generation strategy based on error rate:
       - Low error (< 20%) → model performing well → inject challenging edge cases
       - Medium error (20-40%) → boundary exploration
       - High error (> 40%) → model struggling → send clean data to re-baseline
    5. Log all stats to Loki for Grafana visualization
    """

    SHIPPING_RISK_SCORES = {
        'Same Day': -0.30,
        'First Class': -0.10,
        'Second Class': +0.20,
        'Standard Class': +0.40
    }

    REGION_RISK_SCORES = {
        'Africa': +0.20, 'South Asia': +0.15, 'Southeast Asia': +0.15,
        'Eastern Africa': +0.20, 'West Africa': +0.20,
        'Western Europe': -0.10, 'North America': -0.05,
        'US Center': -0.05, 'USCA': -0.05
    }

    def __init__(self):
        self.prediction_window = deque(maxlen=200)  # Sliding window
        self.total_processed = 0
        self.boundary_case_count = 0
        self.clean_reset_count = 0

    @property
    def error_rate(self) -> float:
        if not self.prediction_window:
            return 0.0
        errors = sum(1 for p, a in self.prediction_window if p != a)
        return errors / len(self.prediction_window)

    @property
    def regime(self) -> str:
        er = self.error_rate
        if er < 0.20:
            return 'CHALLENGING'   # Model is too confident → stress test it
        elif er < 0.40:
            return 'BOUNDARY'      # Model at learning edge → feed boundary cases
        else:
            return 'RESET'         # Model confused → feed clean nominal data

    def heuristic_predict(self, msg: Dict[str, Any]) -> int:
        """
        Rule-based proxy for the trained Spark ML model.
        Derived from known business causal logic and expected RF feature importances.
        """
        score = 0.0

        # Shipping mode (strongest predictor)
        mode = msg.get('Shipping Mode', 'Standard Class')
        score += self.SHIPPING_RISK_SCORES.get(mode, 0.10)

        # Scheduled days (second strongest)
        days = msg.get('Days for shipment (scheduled)', 3) or 3
        if days <= 1:
            score -= 0.25
        elif days >= 6:
            score += 0.35
        elif days >= 4:
            score += 0.15
        elif days == 3:
            score += 0.05

        # Geographic risk
        region = msg.get('Order Region', '')
        score += self.REGION_RISK_SCORES.get(region, 0.0)

        # Financial stress signals
        benefit = msg.get('Benefit per order', 0) or 0
        if benefit < 0:
            score += 0.10
        discount = msg.get('Order Item Discount', 0) or 0
        if discount > 60:
            score += 0.08

        # Customer segment (Corporate = better logistics = lower risk)
        segment = msg.get('Customer Segment', '')
        if segment == 'Corporate':
            score -= 0.05
        elif segment == 'Home Office':
            score += 0.05

        # Order type
        if msg.get('Type') == 'DEBIT':
            score += 0.03

        return 1 if score >= 0.30 else 0

    def process(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processes an event, updates the learning feedback loop, and adapts.

        Returns the (possibly adapted) event to send to Kafka.
        """
        self.total_processed += 1

        # Predict vs actual
        actual = int(msg.get('Late_delivery_risk', 0) or 0)
        predicted = self.heuristic_predict(msg)
        self.prediction_window.append((predicted, actual))

        # Adapt event based on current learning regime
        current_regime = self.regime

        if current_regime == 'CHALLENGING' and random.random() < 0.6:
            # Model doing well → stress test with edge cases
            msg, _ = SmartMixCorruptor._decision_boundary_case(msg)
            self.boundary_case_count += 1
            logger.info(
                f"[AI:CHALLENGING] error_rate={self.error_rate:.3f} | "
                f"Boundary case injected | samples={self.total_processed}"
            )

        elif current_regime == 'BOUNDARY' and random.random() < 0.5:
            # At learning edge → mix of edge cases and anomalies
            msg, anomaly_type = SmartMixCorruptor.inject(msg)
            self.boundary_case_count += 1
            logger.info(
                f"[AI:BOUNDARY] error_rate={self.error_rate:.3f} | "
                f"Anomaly={anomaly_type} | samples={self.total_processed}"
            )

        elif current_regime == 'RESET':
            # Model confused → send clean nominal data to re-baseline
            self.clean_reset_count += 1
            logger.warning(
                f"[AI:RESET] error_rate={self.error_rate:.3f} — "
                f"High confusion detected. Sending nominal data to stabilize. "
                f"samples={self.total_processed}"
            )

        else:
            logger.info(
                f"[AI:NOMINAL] error_rate={self.error_rate:.3f} | "
                f"predicted={predicted} actual={actual} | "
                f"regime={current_regime} | samples={self.total_processed}"
            )

        return msg

    def log_stats(self) -> None:
        """Emits periodic performance stats to Loki."""
        stats = {
            'total_processed': self.total_processed,
            'error_rate': round(self.error_rate, 4),
            'regime': self.regime,
            'boundary_cases': self.boundary_case_count,
            'clean_resets': self.clean_reset_count,
            'window_size': len(self.prediction_window)
        }
        logger.info(f"[AI:STATS] {json.dumps(stats)}")


# ==============================================================================
# ALL-IN-ONE (AIO) PREMIUM MODE
# ==============================================================================
class AIOStreamMode:
    """
    All-In-One premium streaming mode — optimized for maximum model improvement.

    Distribution scientifiquement calibrée pour le Curriculum Learning :
    ┌──────────────────────────────┬──────┬────────────────────────────────────────┐
    │ Catégorie                    │   %  │ Justification ML                       │
    ├──────────────────────────────┼──────┼────────────────────────────────────────┤
    │ Nominal (dataset réel)       │  35% │ Ancre sur la réalité, évite forgetting │
    │ AI Boundary Cases            │  30% │ Uncertainty sampling — max signal/ex   │
    │ Smart Mix (anomalies cohérent│  20% │ Robustesse production réelle           │
    │ Chaos (cas extrêmes)         │  15% │ Couverture longue queue                │
    └──────────────────────────────┴──────┴────────────────────────────────────────┘

    Mécanisme clé : Uncertainty Sampling
        Si le score heuristique d'un événement est proche du seuil de décision
        (zone d'incertitude 0.20–0.40), il est AUTOMATIQUEMENT redirigé vers
        les Boundary Cases, indépendamment du tirage de distribution.
        Ces événements sont les plus précieux pour l'apprentissage.
    """

    # Distribution cible (weights cumulatifs pour tirage rapide)
    DIST_NOMINAL  = 0.35
    DIST_BOUNDARY = 0.65   # 0.35 + 0.30
    DIST_MIX      = 0.85   # 0.65 + 0.20
    # reste = 0.15 chaos

    # Zone d'incertitude du prédicteur heuristique
    UNCERTAINTY_LOW  = 0.20
    UNCERTAINTY_HIGH = 0.40

    def __init__(self, ai_mode: 'AIFluxMode'):
        self.ai_mode = ai_mode
        # Compteurs distribution réelle
        self.counters = {'nominal': 0, 'boundary': 0, 'mix': 0, 'chaos': 0}
        self.total = 0
        self.uncertainty_redirects = 0  # Évts redirigés par uncertainty sampling

    def _raw_heuristic_score(self, msg: Dict[str, Any]) -> float:
        """Retourne le score continu (0-1) avant seuillage."""
        score = 0.5  # Prior neutre

        mode = msg.get('Shipping Mode', 'Standard Class')
        score += AIFluxMode.SHIPPING_RISK_SCORES.get(mode, 0.10)

        days = msg.get('Days for shipment (scheduled)', 3) or 3
        if days <= 1:   score -= 0.25
        elif days >= 6: score += 0.35
        elif days >= 4: score += 0.15

        region = msg.get('Order Region', '')
        score += AIFluxMode.REGION_RISK_SCORES.get(region, 0.0)

        benefit = msg.get('Benefit per order', 0) or 0
        if benefit < 0: score += 0.10

        return max(0.0, min(1.0, score))

    def process(self, msg: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """
        Applique la distribution AIO avec uncertainty sampling prioritaire.
        Retourne (msg_traité, catégorie_appliquée).
        """
        self.total += 1

        # --- Uncertainty Sampling (prioritaire sur la distribution) ---
        score = self._raw_heuristic_score(msg)
        if self.UNCERTAINTY_LOW <= score <= self.UNCERTAINTY_HIGH:
            # Événement incertain → max valeur pédagogique → boundary case
            msg, _ = SmartMixCorruptor._decision_boundary_case(msg)
            self.counters['boundary'] += 1
            self.uncertainty_redirects += 1
            cat = 'boundary_uncertainty'
        else:
            # Distribution normale
            roll = random.random()
            if roll < self.DIST_NOMINAL:
                # Nominal — données réelles propres
                cat = 'nominal'
                self.counters['nominal'] += 1
                self.ai_mode.prediction_window.append(
                    (self.ai_mode.heuristic_predict(msg), int(msg.get('Late_delivery_risk', 0) or 0))
                )
            elif roll < self.DIST_BOUNDARY:
                # AI Boundary — cas frontier générés
                msg, _ = SmartMixCorruptor._decision_boundary_case(msg)
                cat = 'boundary'
                self.counters['boundary'] += 1
            elif roll < self.DIST_MIX:
                # Smart Mix — anomalie causalement cohérente
                msg, anomaly_type = SmartMixCorruptor.inject(msg)
                cat = f'mix:{anomaly_type}'
                self.counters['mix'] += 1
            else:
                # Chaos — cas extrême
                msg, anomaly_type = SmartMixCorruptor.inject(msg)
                # Forcer une anomalie forte pour le chaos
                msg['Order Item Total'] = round((msg.get('Order Item Total', 50) or 50) * -1.5, 2)
                cat = f'chaos:{anomaly_type}'
                self.counters['chaos'] += 1

        return msg, cat

    def log_stats(self) -> None:
        """Émet les stats de distribution réelle vers Loki."""
        total = max(self.total, 1)
        dist_actual = {k: round(v / total * 100, 1) for k, v in self.counters.items()}
        stats = {
            'mode': 'AIO',
            'total': self.total,
            'distribution_target': {'nominal': 35, 'boundary': 30, 'mix': 20, 'chaos': 15},
            'distribution_actual_pct': dist_actual,
            'uncertainty_redirects': self.uncertainty_redirects,
            'ai_error_rate': round(self.ai_mode.error_rate, 4),
            'ai_regime': self.ai_mode.regime
        }
        logger.info(f"[AIO:STATS] {json.dumps(stats)}")


# ==============================================================================
# STREAM MANAGER
# ==============================================================================
class StreamingManager:
    """Manages the lifecycle and execution of the data stream."""

    def __init__(self, mode: str, delay: float = 0.1):
        self.mode = mode
        self._event_delay = delay
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        self.ai_mode = AIFluxMode() if mode in ('ia', 'aio') else None
        self.aio_mode = AIOStreamMode(self.ai_mode) if mode == 'aio' else None

    def _load_and_prepare_dataset(self) -> Optional[pd.DataFrame]:
        """Loads the historical dataset, enforces types, time-projects to today."""
        if not os.path.exists(DATASET_FILE_PATH):
            logger.critical(f"Dataset path {DATASET_FILE_PATH} does not exist.")
            return None

        logger.info(f"Loading historic dataset from {DATASET_FILE_PATH}")
        df = pd.read_csv(DATASET_FILE_PATH, encoding='latin1')

        columns_to_keep = [
            'Order Id', 'Customer Id', 'Product Card Id',
            'order date (DateOrders)', 'shipping date (DateOrders)',
            'Days for shipping (real)', 'Days for shipment (scheduled)',
            'Benefit per order', 'Sales per customer', 'Order Item Total',
            'Order Item Discount', 'Product Price',
            'Order Status', 'Delivery Status', 'Shipping Mode', 'Late_delivery_risk',
            'Customer Country', 'Customer City', 'Order Country', 'Order City',
            'Order Region', 'Category Name', 'Customer Segment', 'Department Name', 'Type'
        ]
        df = df[columns_to_keep]

        # Enforce date types and time-project to current timeline
        df['order date (DateOrders)'] = pd.to_datetime(df['order date (DateOrders)'])
        df['shipping date (DateOrders)'] = pd.to_datetime(df['shipping date (DateOrders)'])

        max_date = df['order date (DateOrders)'].max()
        target_date = datetime(2026, 3, 1)
        offset = target_date - max_date

        df['order date (DateOrders)'] += offset
        df['shipping date (DateOrders)'] += offset
        df = df.sort_values(by='order date (DateOrders)')

        return df

    def execute_stream(self) -> None:
        """Starts the main delivery loop to Kafka Brokers."""
        df = self._load_and_prepare_dataset()
        if df is None:
            return

        logger.info(f"Initiating Live Stream in mode: [{self.mode.upper()}]")

        # Persistent cursor
        start_index = 0
        if os.path.exists(CURSOR_FILE):
            try:
                with open(CURSOR_FILE, 'r') as f:
                    start_index = int(f.read().strip())
                logger.info(f"Resuming from cursor: {start_index}")
            except Exception as e:
                logger.warning(f"Cursor read failed: {e}. Defaulting to 0.")

        df = df.iloc[start_index:]
        stats_log_interval = 50
        event_delay = getattr(args, 'delay', 0.1) if 'args' in dir() else 0.1

        try:
            for idx_offset, (_, row) in enumerate(df.iterrows()):
                msg = row.to_dict()

                # Format dates
                msg['order date (DateOrders)'] = msg['order date (DateOrders)'].strftime('%Y-%m-%d %H:%M:%S')
                msg['shipping date (DateOrders)'] = msg['shipping date (DateOrders)'].strftime('%Y-%m-%d %H:%M:%S')

                # Apply mode logic
                if self.mode == 'chaos':
                    msg, anomaly_type = SmartMixCorruptor.inject(msg)
                    logger.warning(f"[CHAOS] {anomaly_type} | Order {msg.get('Order Id')}")

                elif self.mode == 'mix':
                    if random.choice([True, False]):
                        msg, anomaly_type = SmartMixCorruptor.inject(msg)
                    else:
                        logger.info(f"[NOMINAL] Order {msg.get('Order Id')} dispatched clean.")

                elif self.mode == 'ia':
                    msg = self.ai_mode.process(msg)
                    if (idx_offset + 1) % stats_log_interval == 0:
                        self.ai_mode.log_stats()

                elif self.mode == 'aio':
                    msg, cat = self.aio_mode.process(msg)
                    logger.info(f"[AIO:{cat.upper().split(':')[0]}] Order {msg.get('Order Id')} | cat={cat}")
                    if (idx_offset + 1) % stats_log_interval == 0:
                        self.aio_mode.log_stats()

                else:  # 'sain'
                    logger.info(f"[NOMINAL] Order {msg.get('Order Id')} dispatched.")

                self.producer.send(KAFKA_TOPIC, msg)

                # Update persistent cursor
                with open(CURSOR_FILE, 'w') as f:
                    f.write(str(start_index + idx_offset + 1))

                time.sleep(self._event_delay)

        except KeyboardInterrupt:
            logger.info("Stream gracefully terminated by operator.")

        except Exception as e:
            logger.error(f"Stream critical failure: {e}", exc_info=True)

        finally:
            self.producer.flush()
            self.producer.close()
            if self.aio_mode:
                self.aio_mode.log_stats()
                logger.info(
                    f"[AIO:FINAL] {self.aio_mode.total} events | "
                    f"Uncertainty redirects={self.aio_mode.uncertainty_redirects} | "
                    f"AI error_rate={self.ai_mode.error_rate:.4f}"
                )
            elif self.ai_mode:
                self.ai_mode.log_stats()
                logger.info(
                    f"[AI:FINAL] Processed {self.ai_mode.total_processed} events | "
                    f"Final error_rate={self.ai_mode.error_rate:.4f} | "
                    f"Boundary cases={self.ai_mode.boundary_case_count} | "
                    f"Clean resets={self.ai_mode.clean_reset_count}"
                )


# ==============================================================================
# ENTRY POINT
# ==============================================================================
def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Logistics 4.0 Streaming Data Manager")
    parser.add_argument(
        '--mode',
        choices=['sain', 'chaos', 'mix', 'ia', 'aio'],
        required=True,
        help=(
            "Stream execution modes:\n"
            "  sain  — Clean nominal data\n"
            "  chaos — 100%% Smart Mix anomalies\n"
            "  mix   — 50%% Smart Mix anomalies (causally coherent)\n"
            "  ia    — AI self-learning: adapts generation from prediction errors\n"
            "  aio   — All-In-One: 35%% nominal / 30%% AI boundary / 20%% mix / 15%% chaos"
        )
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.1,
        help='Delay in seconds between events (default: 0.1 = 10 events/sec)'
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    manager = StreamingManager(mode=args.mode, delay=args.delay)
    manager.execute_stream()
