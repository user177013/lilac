<script lang="ts">
  import {
    infiniteQuerySelectRows,
    queryDatasetSchema,
    querySelectRowsSchema
  } from '$lib/queries/datasetQueries';
  import {
    getDatasetViewContext,
    getSelectRowsOptions,
    getSelectRowsSchemaOptions
  } from '$lib/stores/datasetViewStore';
  import {getDisplayPath, getHighlightedFields} from '$lib/view_utils';
  import {
    childFields,
    getField,
    L,
    pathIncludes,
    ROWID,
    serializePath,
    valueAtPath,
    type LilacField,
    type Path
  } from '$osmanthus';
  import {
    DataTable,
    InlineNotification,
    SkeletonText
  } from 'carbon-components-svelte';
  import {ChevronDown, ChevronRight} from 'carbon-icons-svelte';
  import InfiniteScroll from 'svelte-infinite-scroll';

  import DatasetControls from './DatasetControls.svelte';
  import DatasetTableCell from './DatasetTableCell.svelte';

  const datasetViewStore = getDatasetViewContext();

  $: schema = queryDatasetSchema($datasetViewStore.namespace, $datasetViewStore.datasetName);
  $: selectOptions = getSelectRowsOptions($datasetViewStore, $schema.data);

  $: selectRowsSchema = querySelectRowsSchema(
    $datasetViewStore.namespace,
    $datasetViewStore.datasetName,
    getSelectRowsSchemaOptions($datasetViewStore, $schema.data)
  );

  $: rowsQuery = infiniteQuerySelectRows(
    $datasetViewStore.namespace,
    $datasetViewStore.datasetName,
    {...selectOptions, combine_columns: true},
    $selectRowsSchema?.isSuccess ? $selectRowsSchema.data.schema : undefined
  );
  
  $: highlightedFields = getHighlightedFields($datasetViewStore.query, $selectRowsSchema.data);

  $: totalNumRows = $rowsQuery.data?.pages[0].total_num_rows;

  $: flatRows = $rowsQuery.data?.pages.flatMap(page => page.rows) || [];

  $: headers = $schema.isSuccess
    ? childFields($schema.data)
        .filter(f => f.dtype != null) // Only leaf fields
        .filter(f => {
          const pathStr = f.path.join('.').toLowerCase();
          
          // Check if this field is part of an active search
          const isActiveSearch = ($datasetViewStore.query.searches || []).some(s => 
            pathIncludes(f.path, s.path)
          );

          // Exclude metadata, signal results, and enrichments (unless they are active searches)
          const isMetadata = (
            pathStr.includes('cluster') ||
            pathStr.includes('embed') ||
            pathStr.includes('pii') ||
            (pathStr.includes('signal') && !isActiveSearch) ||
            (pathStr.includes('computed') && !isActiveSearch) ||
            pathStr.includes('__hfsplit__') ||
            pathStr.includes('__timestamp__') ||
            pathStr.includes('concept') ||
            f.path[0].toString().startsWith('_') // Hidden root fields
          );
          return !isMetadata;
        })
        .map(f => ({
          key: JSON.stringify(f.path),
          value: getDisplayPath(f.path),
          dtype: Object.keys(f.dtype || {}).find(k => k !== 'type') || f.dtype?.type || 'unknown'
        }))
    : [];

  $: tableRows = flatRows.map((row, i) => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const tableRow: any = {id: i}; // DataTable needs an 'id' field
    headers.forEach(header => {
      const path = JSON.parse(header.key);
      const val = valueAtPath(row, path);
      tableRow[header.key] = val != null ? ((L.value(val) as string | number) ?? '') : '';
    });
    return tableRow;
  });

  function onRowClick(e: CustomEvent<{id: number}>) {
    const i = e.detail.id;
    const row = flatRows[i];
    if (row) {
      const val = valueAtPath(row, [ROWID]);
      $datasetViewStore.rowId = val != null ? ((L.value(val) as string) ?? undefined) : undefined;
    }
  }

  function getDtype(header: unknown) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (header as any).dtype;
  }

  $: selectedId = $datasetViewStore.rowId;
  // Map rowId to table row index for highlighting
  $: selectedRowId = flatRows.findIndex(row => {
    const val = valueAtPath(row, [ROWID]);
    return val != null && L.value(val) === selectedId;
  });

  let expandedCells = new Set<string>();
  function toggleExpand(rowId: number, colKey: string) {
    const key = `${rowId}-${colKey}`;
    if (expandedCells.has(key)) {
      expandedCells.delete(key);
    } else {
      expandedCells.add(key);
    }
    expandedCells = expandedCells; // trigger update
  }

  function cellToPath(cellKey: string): Path {
    return (JSON.parse(cellKey) as Path).map(p => p.toString().replace(/\[(\d+)\]/g, '$1'));
  }

  let scrollContainer: HTMLDivElement;
</script>

<div class="flex h-full w-full flex-col overflow-hidden px-4 pt-4 pb-8">
  <div class="mb-4 flex-shrink-0">
    <DatasetControls numRowsInQuery={totalNumRows} />
  </div>
  {#if $rowsQuery.isError}
    <div class="p-4">
      <InlineNotification
        lowContrast
        title="Error"
        subtitle={$rowsQuery.error.body?.detail || $rowsQuery.error.message}
      />
    </div>
  {:else if $schema.isSuccess}
    <div
      class="table-container flex-grow h-full overflow-y-auto border border-neutral-200"
      bind:this={scrollContainer}
    >
      <DataTable
        on:click:row={onRowClick}
        {headers}
        rows={tableRows}
        selectedRowIds={selectedRowId >= 0 ? [selectedRowId] : []}
      >
        <svelte:fragment slot="cell-header" let:header>
          <div class="header-content flex flex-col py-2">
            <span class="text-osmanthus-floral font-semibold">{header.value}</span>
            <span class="text-xs font-normal opacity-70 italic text-osmanthus-floral">{getDtype(header)}</span>
          </div>
        </svelte:fragment>
        <svelte:fragment slot="cell" let:cell let:row>
          {@const path = cellToPath(cell.key)}
          <!-- svelte-ignore a11y-click-events-have-key-events -->
          <div 
            class="cell-content whitespace-pre-wrap py-2 text-sm leading-relaxed cursor-pointer" 
            class:expanded={expandedCells.has(`${row.id}-${cell.key}`)}
            on:click|stopPropagation={() => toggleExpand(row.id, cell.key)}
          >
            <DatasetTableCell
              value={cell.value}
              row={flatRows[row.id]}
              field={getField($selectRowsSchema.data?.schema, path)}
              {highlightedFields}
            />
          </div>
        </svelte:fragment>
      </DataTable>
      {#if flatRows.length < (totalNumRows || 0)}
        <InfiniteScroll elementScroll={scrollContainer} threshold={200} on:loadMore={() => $rowsQuery.fetchNextPage()} />
      {/if}
    </div>
  {/if}

  {#if $rowsQuery.isFetching}
    <div class="py-2">
      <SkeletonText />
    </div>
  {/if}
</div>

<style lang="postcss">
  .table-container {
    @apply w-full h-full;
  }
  .table-container :global(.bx--data-table-container) {
    @apply h-auto min-h-full p-0 overflow-visible;
  }
  
  .table-container :global(.bx--data-table) {
    @apply w-full table-auto border-separate border-spacing-0;
  }
  
  /* Manual Stickiness to avoid Carbon's max-height bugs */
  .table-container :global(.bx--data-table thead) {
    @apply sticky top-0 z-10;
  }
  
  /* Override Carbon's strict heights */
  .table-container :global(.bx--data-table thead tr),
  .table-container :global(.bx--data-table tbody tr) {
    height: auto !important;
  }

  .table-container :global(.bx--data-table thead th) {
    @apply text-osmanthus-floral border-b border-neutral-700 bg-osmanthus-charcoal px-4 py-4 align-top;
    height: auto !important;
    min-width: 250px;
    max-width: 450px; 
  }
  
  .table-container :global(.bx--data-table tbody td) {
    @apply border-b border-neutral-200 px-4 py-4 align-top shadow-none;
    height: auto !important;
    min-width: 250px;
  }

  .cell-content {
    display: -webkit-box;
    -webkit-line-clamp: 4; /* Slightly more spacious default */
    -webkit-box-orient: vertical;
    overflow: hidden;
    line-height: 1.6;
    max-width: 500px;
    white-space: pre-wrap;
    @apply transition-all duration-200;
  }
  .cell-content.expanded {
    display: block;
    overflow: visible;
    -webkit-line-clamp: unset;
    max-width: 900px;
  }

  /* Row Alternation & Interaction */
  .table-container :global(.bx--data-table tbody tr:nth-child(even)) {
    @apply bg-osmanthus-floral;
  }
  .table-container :global(.bx--data-table tbody tr) {
    @apply cursor-pointer transition-colors duration-150;
  }
  .table-container :global(.bx--data-table tbody tr:hover) {
    @apply bg-osmanthus-apricot;
  }
  
  /* Selection Highlight */
  .table-container :global(.bx--data-table tbody tr.bx--data-table--selected) {
    @apply bg-osmanthus-gold/20;
    box-shadow: inset 4px 0 0 #F59E0B;
  }
</style>
