'use client';

import { useState } from 'react';
import { useProgramDetail } from '@/hooks/use-programs';
import { ProgramStats } from '@/components/programs/program-stats';
import { ProgramChart } from '@/components/programs/program-chart';
import { ArrowLeft, ExternalLink, Copy, Check } from 'lucide-react';
import Link from 'next/link';
import { SOLANA_EXPLORER_URL } from '@/lib/constants';
import { formatRelativeTime } from '@/lib/utils/format';

interface ProgramDetailPageProps {
  params: {
    id: string;
  };
}

export default function ProgramDetailPage({ params }: ProgramDetailPageProps) {
  const [copied, setCopied] = useState(false);
  const { data: program, isLoading, error } = useProgramDetail(params.id, '7d');

  function copyToClipboard() {
    navigator.clipboard.writeText(program?.program_id || '');
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }

  if (isLoading) {
    return (
      <div className="min-h-screen bg-bg1 overflow-x-clip relative">
        <div className="absolute top-0 left-0 right-0 h-[400px] bg-gradient-to-b from-white/50 to-transparent pointer-events-none z-0" />
        <div className="relative z-10">
          <div className="max-w-7xl mx-auto border-r border-l border-border-low">
            <section className="relative py-16 px-8">
              <div className="space-y-6">
                <div className="h-10 w-40 bg-gray-200 animate-pulse rounded" />
                <div className="h-12 w-full max-w-3xl bg-gray-200 animate-pulse rounded" />
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                  {Array.from({ length: 4 }).map((_, i) => (
                    <div key={i} className="h-32 bg-gray-200 animate-pulse rounded" />
                  ))}
                </div>
                <div className="h-96 bg-gray-200 animate-pulse rounded" />
              </div>
            </section>
          </div>
        </div>
      </div>
    );
  }

  if (error || !program) {
    return (
      <div className="min-h-screen bg-bg1 overflow-x-clip relative">
        <div className="absolute top-0 left-0 right-0 h-[400px] bg-gradient-to-b from-white/50 to-transparent pointer-events-none z-0" />
        <div className="relative z-10">
          <div className="max-w-7xl mx-auto border-r border-l border-border-low">
            <section className="relative py-16 px-8">
              <Link href="/programs">
                <button className="flex items-center gap-2 text-body-md text-gray-600 hover:text-gray-900 transition-colors mb-8">
                  <ArrowLeft className="h-4 w-4" />
                  Back to Programs
                </button>
              </Link>
              
              <div className="group relative p-12 border border-border-low bg-white max-w-2xl mx-auto text-center">
                <div className="product-card-diagonal absolute inset-0 pointer-events-none" />
                <div className="relative z-10">
                  <h2 className="text-h3 mb-3">Program Not Found</h2>
                  <p className="text-body-l text-gray-600 mb-6">
                    {error?.message || 'Unable to load program details. The program may not exist or there was an error fetching the data.'}
                  </p>
                  <Link href="/programs">
                    <button className="px-6 py-3 bg-gray-900 text-white text-body-md hover:bg-gray-800 transition-colors">
                      <ArrowLeft className="inline-block mr-2 h-4 w-4" />
                      Back to Programs
                    </button>
                  </Link>
                </div>
              </div>
            </section>
          </div>
        </div>
      </div>
    );
  }

  const successRate = program.success_rate;
  const statusColor = successRate >= 95 ? 'green' : successRate >= 80 ? 'yellow' : 'red';

  return (
    <div className="min-h-screen bg-bg1 overflow-x-clip relative">
      {/* Gradient from top */}
      <div className="absolute top-0 left-0 right-0 h-[400px] bg-gradient-to-b from-white/50 to-transparent pointer-events-none z-0" />
      
      <div className="relative z-10">
        <div className="max-w-7xl mx-auto border-r border-l border-border-low">
          {/* Header Section */}
          <section className="relative py-16 px-8 border-b border-border-low">
            <svg 
              className="absolute inset-0 pointer-events-none z-10"
              style={{
                left: '50%',
                top: '0%',
                transform: 'translate(-50%, -50%)',
                width: '150vw',
                height: '100vh',
              }}
            >
              <defs>
                <linearGradient id="programDetailPulse" x1="0%" y1="0%" x2="100%" y2="0%">
                  <stop offset="0%" stopColor="transparent" />
                  <stop offset="50%" stopColor="rgba(0, 0, 0, 0.8)" />
                  <stop offset="100%" stopColor="transparent" />
                </linearGradient>
              </defs>
              
              <line 
                x1="0" 
                y1="50%" 
                x2="100%" 
                y2="50%" 
                stroke="rgba(233, 231, 222, 1)" 
                strokeWidth="1"
              />
              
              <rect 
                x="0" 
                y="50%" 
                width="2%" 
                height="1" 
                fill="url(#programDetailPulse)"
                transform="translate(0, -0.5)"
              >
                <animate
                  attributeName="x"
                  values="0; 100%; 100%"
                  dur="6s"
                  begin="0.5s"
                  repeatCount="indefinite"
                />
                <animate
                  attributeName="opacity"
                  values="0; 0.8; 0.8; 0"
                  dur="6s"
                  begin="0.5s"
                  repeatCount="indefinite"
                />
              </rect>
            </svg>

            <div className="relative z-20">
              <Link href="/programs">
                <button className="flex items-center gap-2 text-body-md text-gray-600 hover:text-gray-900 transition-colors mb-6">
                  <ArrowLeft className="h-4 w-4" />
                  Back to Programs
                </button>
              </Link>

              <div className="flex flex-col lg:flex-row lg:items-start lg:justify-between gap-6">
                <div className="flex-1">
                  <h1 className="text-h2 mb-4">Program Analytics</h1>
                  
                  {/* Program ID with Copy */}
                  <div className="flex items-center gap-3 flex-wrap mb-4">
                    <code className="text-body-md font-mono bg-gray-100 px-4 py-2 border border-border-low break-all max-w-full">
                      {program.program_id}
                    </code>
                    <button
                      onClick={copyToClipboard}
                      className="px-3 py-2 border border-border-low bg-white hover:bg-gray-50 transition-colors text-body-md flex items-center gap-2"
                    >
                      {copied ? (
                        <>
                          <Check className="h-4 w-4" />
                          Copied
                        </>
                      ) : (
                        <>
                          <Copy className="h-4 w-4" />
                          Copy
                        </>
                      )}
                    </button>
                  </div>

                  {/* Status Badge */}
                  <div className="flex items-center gap-3 mb-3">
                    <div className={`flex items-center gap-2 px-3 py-1.5 border rounded-full ${
                      statusColor === 'green' ? 'bg-green-50 border-green-200' :
                      statusColor === 'yellow' ? 'bg-yellow-50 border-yellow-200' :
                      'bg-red-50 border-red-200'
                    }`}>
                      <span className={`inline-block w-2 h-2 rounded-full ${
                        statusColor === 'green' ? 'bg-green-500' :
                        statusColor === 'yellow' ? 'bg-yellow-500' :
                        'bg-red-500'
                      }`}></span>
                      <span className={`text-sm font-medium ${
                        statusColor === 'green' ? 'text-green-700' :
                        statusColor === 'yellow' ? 'text-yellow-700' :
                        'text-red-700'
                      }`}>
                        {program.success_rate.toFixed(1)}% Success Rate
                      </span>
                    </div>
                  </div>

                  {/* Last Activity */}
                  {program.last_seen && (
                    <p className="text-body-md text-gray-600">
                      Last activity {formatRelativeTime(program.last_seen)}
                    </p>
                  )}
                </div>

                {/* Action Buttons */}
                <div className="flex gap-3 flex-wrap">
                  <a
                    href={`${SOLANA_EXPLORER_URL}/address/${program.program_id}`}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    <button className="px-6 py-3 border border-border-low bg-white hover:bg-gray-50 transition-colors text-body-md flex items-center gap-2">
                      <ExternalLink className="h-4 w-4" />
                      Solana Explorer
                    </button>
                  </a>
                </div>
              </div>
            </div>
          </section>

          {/* Stats Section */}
          <section className="relative py-16 px-8">
            <div className="absolute inset-0 left-[50%] -translate-x-1/2 w-screen z-[0] bg-gradient-to-t from-sand-200/50 to-transparent" />
            
            <div className="max-w-7xl mx-auto relative z-10 space-y-8">
              {/* Stats Cards */}
              <ProgramStats program={program} />

              {/* Activity Chart */}
              {program.hourly_stats && program.hourly_stats.length > 0 ? (
                <ProgramChart data={program.hourly_stats} />
              ) : (
                <div className="group relative p-12 border border-border-low bg-white text-center">
                  <div className="product-card-diagonal absolute inset-0 pointer-events-none" />
                  <div className="relative z-10">
                    <h3 className="text-h4 mb-3">Activity Over Time</h3>
                    <p className="text-body-l text-gray-600">
                      No historical activity data available for this program
                    </p>
                  </div>
                </div>
              )}
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}

