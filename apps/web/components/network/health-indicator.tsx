'use client';

import { Badge } from '@/components/ui/badge';
import { CheckCircle2, AlertCircle, XCircle } from 'lucide-react';

interface HealthIndicatorProps {
  status: 'healthy' | 'degraded' | 'down';
  latency?: number;
}

export function HealthIndicator({ status, latency }: HealthIndicatorProps) {
  const config = {
    healthy: {
      variant: 'success' as const,
      icon: CheckCircle2,
      label: 'Healthy',
      color: 'text-green-500',
    },
    degraded: {
      variant: 'warning' as const,
      icon: AlertCircle,
      label: 'Degraded',
      color: 'text-yellow-500',
    },
    down: {
      variant: 'destructive' as const,
      icon: XCircle,
      label: 'Down',
      color: 'text-red-500',
    },
  };

  const { variant, icon: Icon, label, color } = config[status];

  return (
    <div className="flex items-center gap-2">
      <Icon className={`h-5 w-5 ${color}`} />
      <Badge variant={variant}>{label}</Badge>
      {latency !== undefined && (
        <span className="text-sm text-muted-foreground">
          {latency}ms
        </span>
      )}
    </div>
  );
}

