<script lang="ts">
  import {
    L,
    pathIncludes,
    pathMatchesPrefix,
    serializePath,
    getValueNodes,
    type LilacField,
    type LilacValueNode,
    type LilacValueNodeCasted
  } from '$osmanthus';
  import {getSpanValuePaths, mergeSpans} from '$lib/view_utils';
  import {getRenderSpans, type SpanValueInfo} from './spanHighlight';
  import {hoverTooltip} from '../common/HoverTooltip';
  import SpanHoverTooltip from './SpanHoverTooltip.svelte';
  import {getDatasetViewContext} from '$lib/stores/datasetViewStore';

  export let value: string | number | boolean | null;
  export let row: LilacValueNode | undefined | null;
  export let field: LilacField | undefined;
  export let highlightedFields: LilacField[];

  const datasetViewStore = getDatasetViewContext();

  $: text = value?.toString() || '';

  $: spanValuePaths = field
    ? getSpanValuePaths(field, highlightedFields)
    : {spanPaths: [], spanValueInfos: []};

  $: pathToSpans = (() => {
    const result: Record<string, LilacValueNodeCasted<'string_span'>[]> = {};
    if (!row || !field) return result;
    
    for (const sp of spanValuePaths.spanPaths) {
      let valueNodes = getValueNodes(row, sp);
      if (valueNodes == null) continue;
      
      const isSpanNestedUnder = pathMatchesPrefix(sp, field.path);
      if (isSpanNestedUnder) {
        // Filter out any span values that do not share the same coordinates as the current path we
        // are rendering.
        valueNodes = valueNodes.filter(v => pathIncludes(L.path(v)!, field.path));
      }
      result[serializePath(sp)] = valueNodes as LilacValueNodeCasted<'string_span'>[];
    }
    return result;
  })();

  $: spanPathToValueInfos = (() => {
    const result: Record<string, SpanValueInfo[]> = {};
    for (const info of spanValuePaths.spanValueInfos) {
      const s = serializePath(info.spanPath);
      result[s] = result[s] || [];
      result[s].push(info);
    }
    return result;
  })();

  $: mergedSpans = mergeSpans(text, pathToSpans);
  $: renderSpans = getRenderSpans(mergedSpans, spanPathToValueInfos, new Set()).map(s => {
    // Override color for search highlights to match Osmanthus theme (yellow/gold)
    const isSearch = s.namedValues.some(v => 
      ['semantic_similarity', 'keyword', 'concept_score'].includes(v.info.type)
    );
    if (isSearch && s.isHighlighted) {
      // Use osmanthus-gold (#F59E0B) with some opacity and bold text
      return {
        ...s, 
        backgroundColor: 'rgba(245, 158, 11, 0.25)', 
        isBlackBolded: true 
      };
    }
    return s;
  });
</script>

<span class="cell-wrapper">
  {#each renderSpans as span}
    {#if span.isHighlighted}
      <span
        class="highlighted-span"
        class:font-bold={span.isBlackBolded}
        class:is-search={span.backgroundColor.includes('245, 158, 11')}
        style:background-color={span.backgroundColor}
        use:hoverTooltip={{
          component: SpanHoverTooltip,
          props: {namedValues: span.namedValues}
        }}
      >
        {span.text}
      </span>
    {:else}
      <span>{span.text}</span>
    {/if}
  {/each}
</span>

<style lang="postcss">
  .highlighted-span {
    @apply rounded-sm px-0.5 transition-all duration-150;
  }
  .highlighted-span.is-search {
    @apply border-b-2 border-osmanthus-gold/40;
  }
  .cell-wrapper {
    display: inline;
    @apply text-osmanthus-charcoal;
  }
</style>
