import React, { useState, useEffect } from 'react';
import { Box, Text, useApp, useInput } from 'ink';
import { Manifest, TableEntry, UserDecision } from '../types.js';
import { Header } from './Header.js';
import { Progress } from './Progress.js';
import { TableStatus } from './TableStatus.js';
import { FailurePrompt } from './FailurePrompt.js';
import { Summary } from './Summary.js';

export type AppPhase = 'connecting' | 'enumerating' | 'processing' | 'complete' | 'error' | 'failure-prompt';

export interface AppProps {
  onStart: () => Promise<void>;
  onUserDecision: (decision: UserDecision) => void;
  manifest: Manifest | null;
  currentTable: TableEntry | null;
  currentPhase: 'discovery' | 'extraction' | 'validation' | null;
  appPhase: AppPhase;
  error: string | null;
  failedTable: TableEntry | null;
}

export const App: React.FC<AppProps> = ({
  onStart,
  onUserDecision,
  manifest,
  currentTable,
  currentPhase,
  appPhase,
  error,
  failedTable,
}) => {
  const { exit } = useApp();
  const [started, setStarted] = useState(false);

  useEffect(() => {
    if (!started) {
      setStarted(true);
      onStart().catch((err) => {
        console.error('Fatal error:', err);
        exit();
      });
    }
  }, [started, onStart, exit]);

  useInput((input, key) => {
    if (appPhase === 'failure-prompt') {
      if (input.toLowerCase() === 'c') {
        onUserDecision('continue');
      } else if (input.toLowerCase() === 'r') {
        onUserDecision('retry');
      } else if (input.toLowerCase() === 'a') {
        onUserDecision('abort');
      }
    }

    if (key.escape || (key.ctrl && input === 'c')) {
      exit();
    }
  });

  return (
    <Box flexDirection="column" padding={1}>
      <Header />

      {appPhase === 'connecting' && (
        <Box marginTop={1}>
          <Text color="yellow">Connecting to database...</Text>
        </Box>
      )}

      {appPhase === 'enumerating' && (
        <Box marginTop={1}>
          <Text color="yellow">Enumerating tables from SAGE linked server...</Text>
        </Box>
      )}

      {appPhase === 'processing' && manifest && (
        <>
          <Progress manifest={manifest} />
          {currentTable && currentPhase && (
            <TableStatus table={currentTable} phase={currentPhase} />
          )}
        </>
      )}

      {appPhase === 'failure-prompt' && failedTable && (
        <FailurePrompt table={failedTable} />
      )}

      {appPhase === 'complete' && manifest && (
        <Summary manifest={manifest} />
      )}

      {appPhase === 'error' && (
        <Box marginTop={1}>
          <Text color="red">Error: {error}</Text>
        </Box>
      )}
    </Box>
  );
};
