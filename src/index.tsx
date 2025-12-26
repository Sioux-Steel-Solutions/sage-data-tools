#!/usr/bin/env node
import React, { useState, useCallback } from 'react';
import { render } from 'ink';
import { App, AppPhase } from './components/App.js';
import { runExtraction } from './extractor.js';
import { connect, enumerateTables } from './db.js';
import { loadManifest, saveManifest } from './manifest.js';
import type { Manifest, TableEntry, UserDecision } from './types.js';

const Main: React.FC = () => {
  const [manifest, setManifest] = useState<Manifest | null>(null);
  const [currentTable, setCurrentTable] = useState<TableEntry | null>(null);
  const [currentPhase, setCurrentPhase] = useState<'discovery' | 'extraction' | 'validation' | null>(null);
  const [appPhase, setAppPhase] = useState<AppPhase>('connecting');
  const [error, setError] = useState<string | null>(null);
  const [failedTable, setFailedTable] = useState<TableEntry | null>(null);
  const [userDecisionResolver, setUserDecisionResolver] = useState<((decision: UserDecision) => void) | null>(null);

  const handleUserDecision = useCallback((decision: UserDecision) => {
    if (userDecisionResolver) {
      userDecisionResolver(decision);
      setUserDecisionResolver(null);
      setFailedTable(null);
      setAppPhase('processing');
    }
  }, [userDecisionResolver]);

  const handleStart = useCallback(async () => {
    try {
      // Phase 1: Connect
      setAppPhase('connecting');
      await connect();

      // Phase 2: Enumerate (if needed)
      setAppPhase('enumerating');
      let currentManifest = await loadManifest();

      if (currentManifest.tables.length === 0) {
        const tables = await enumerateTables();
        currentManifest.tables = tables.map((t) => ({
          name: t.name,
          type: t.type,
          status: 'pending' as const,
        }));
        await saveManifest(currentManifest);
      }

      setManifest(currentManifest);
      setAppPhase('processing');

      // Phase 3: Run extraction
      const finalManifest = await runExtraction(
        // onPhaseChange
        (table, phase) => {
          setCurrentTable({ ...table });
          setCurrentPhase(phase);
        },
        // onFailure
        async (table) => {
          return new Promise<UserDecision>((resolve) => {
            setFailedTable({ ...table });
            setAppPhase('failure-prompt');
            setUserDecisionResolver(() => resolve);
          });
        },
        // onProgress
        (updatedManifest) => {
          setManifest({ ...updatedManifest });
        }
      );

      setManifest(finalManifest);
      setAppPhase('complete');
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setAppPhase('error');
    }
  }, []);

  return (
    <App
      onStart={handleStart}
      onUserDecision={handleUserDecision}
      manifest={manifest}
      currentTable={currentTable}
      currentPhase={currentPhase}
      appPhase={appPhase}
      error={error}
      failedTable={failedTable}
    />
  );
};

render(<Main />);
